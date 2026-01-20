#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MySQL 마이그레이션 검증기
- 스키마 비교(SHOW CREATE TABLE)
- 인덱스 비교(information_schema.statistics)
- 행 수 비교
- 선택: 데이터 해시 비교(샘플 또는 PK 범위 청크)

테스트 환경: mysql-connector-python.

참고 문서:
- MySQL information_schema: MySQL 공식 레퍼런스 매뉴얼
- SHOW CREATE TABLE: MySQL 공식 레퍼런스 매뉴얼
- mysql-connector-python: Oracle MySQL Connector/Python 문서
"""

import argparse
import json
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import mysql.connector


@dataclass
class DbCfg:
    host: str
    port: int
    user: str
    password: str
    database: str


def connect(cfg: DbCfg):
    return mysql.connector.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        autocommit=True,
    )


def fetch_one(conn, sql: str, params: Tuple = ()) -> Optional[Tuple]:
    cur = conn.cursor()
    cur.execute(sql, params)
    row = cur.fetchone()
    cur.close()
    return row


def fetch_all(conn, sql: str, params: Tuple = ()) -> List[Tuple]:
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    return rows


def list_tables(conn, db: str) -> List[str]:
    rows = fetch_all(
        conn,
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (db,),
    )
    return [r[0] for r in rows]


def show_create_table(conn, table: str) -> str:
    row = fetch_one(conn, f"SHOW CREATE TABLE `{table}`")
    if not row:
        return ""
    # row = (Table, Create Table)
    return row[1]


def normalize_ddl(ddl: str) -> str:
    """
    불필요한 차이를 줄이기 위한 가벼운 정규화.
    - 줄 끝 공백 제거
    - 다중 공백 유지(의미 보존)
    주의: 의미 변경은 의도적으로 하지 않음.
    """
    lines = [ln.rstrip() for ln in ddl.splitlines()]
    return "\n".join(lines).strip()

def list_columns(conn, db: str, table: str) -> List[str]:
    rows = fetch_all(
        conn,
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (db, table),
    )
    return [r[0] for r in rows]


def get_indexes(conn, db: str, table: str) -> List[Tuple]:
    """
    (index_name, non_unique, seq_in_index, column_name, collation, sub_part) 목록 반환.
    """
    return fetch_all(
        conn,
        """
        SELECT index_name, non_unique, seq_in_index, column_name, collation, sub_part
        FROM information_schema.statistics
        WHERE table_schema = %s AND table_name = %s
        ORDER BY index_name, seq_in_index
        """,
        (db, table),
    )

def detect_order_keys(conn, db: str, table: str) -> List[str]:
    """
    안정적 정렬을 위한 키 후보를 찾음.
    우선순위: PK -> UNIQUE 인덱스 -> 첫 번째 컬럼.
    """
    pk_cols = fetch_all(
        conn,
        """
        SELECT column_name
        FROM information_schema.key_column_usage
        WHERE table_schema = %s AND table_name = %s AND constraint_name = 'PRIMARY'
        ORDER BY ordinal_position
        """,
        (db, table),
    )
    if pk_cols:
        return [r[0] for r in pk_cols]

    unique_idx = fetch_all(
        conn,
        """
        SELECT index_name, column_name, seq_in_index
        FROM information_schema.statistics
        WHERE table_schema = %s AND table_name = %s AND non_unique = 0
        ORDER BY index_name, seq_in_index
        """,
        (db, table),
    )
    if unique_idx:
        first_name = unique_idx[0][0]
        cols = [r[1] for r in unique_idx if r[0] == first_name]
        if cols:
            return cols

    cols = list_columns(conn, db, table)
    return cols[:1] if cols else []


def count_rows_exact(conn, table: str) -> int:
    row = fetch_one(conn, f"SELECT COUNT(*) FROM `{table}`")
    return int(row[0]) if row else -1


def detect_primary_key(conn, db: str, table: str) -> Optional[str]:
    rows = fetch_all(
        conn,
        """
        SELECT column_name
        FROM information_schema.key_column_usage
        WHERE table_schema = %s AND table_name = %s AND constraint_name = 'PRIMARY'
        ORDER BY ordinal_position
        """,
        (db, table),
    )
    if not rows:
        return None
    # 복합 PK는 처리 복잡도가 높아 단일 컬럼 PK만 지원.
    if len(rows) == 1:
        return rows[0][0]
    return None


def pk_min_max(conn, table: str, pk: str) -> Tuple[Optional[int], Optional[int]]:
    row = fetch_one(conn, f"SELECT MIN(`{pk}`), MAX(`{pk}`) FROM `{table}`")
    if not row:
        return None, None
    return row[0], row[1]

def build_row_expr(columns: List[str]) -> str:
    """
    NULL을 명확히 표현하고, 컬럼 순서를 고정하여 문자열로 직렬화.
    """
    if not columns:
        return "''"
    parts = [f"IFNULL(CAST(`{c}` AS CHAR), '<<NULL>>')" for c in columns]
    return f"CONCAT_WS('#', {', '.join(parts)})"

def checksum_query(
    table: str,
    row_expr: str,
    where_sql: str = "",
    order_sql: str = "",
    limit_sql: str = "",
) -> str:
    return f"""
        SELECT
            COUNT(*) AS row_count,
            COALESCE(SUM(CRC32(row_str)), 0) AS crc_sum,
            COALESCE(BIT_XOR(CRC32(row_str)), 0) AS crc_xor
        FROM (
            SELECT {row_expr} AS row_str
            FROM `{table}` t
            {where_sql}
            {order_sql}
            {limit_sql}
        ) x
    """


def hash_sample(conn, table: str, limit: int = 1000) -> str:
    """
    샘플 해시: 정렬된 일부 행을 기반으로 체크섬 집계.
    주의: 안정적 정렬이 필요함. PK/UNIQUE 기반 정렬을 우선 사용.
    """
    cols = list_columns(conn, conn.database, table)
    order_cols = detect_order_keys(conn, conn.database, table)
    order_sql = ""
    if order_cols:
        order_sql = "ORDER BY " + ", ".join([f"`{c}`" for c in order_cols])
    limit_sql = "LIMIT %s"
    row_expr = build_row_expr(cols)
    row = fetch_one(conn, checksum_query(table, row_expr, order_sql=order_sql, limit_sql=limit_sql), (limit,))
    if not row:
        return ""
    return f"{row[0]}:{row[1]}:{row[2]}"


def hash_pk_range(conn, table: str, pk: str, start: int, end: int) -> str:
    """
    범위 해시: PK가 start~end 사이인 모든 행의 체크섬 집계.
    GROUP_CONCAT 대신 CRC32 집계로 길이 제한 문제를 회피.
    """
    cols = list_columns(conn, conn.database, table)
    row_expr = build_row_expr(cols)
    where_sql = f"WHERE `{pk}` BETWEEN %s AND %s"
    row = fetch_one(conn, checksum_query(table, row_expr, where_sql=where_sql), (start, end))
    if not row:
        return ""
    return f"{row[0]}:{row[1]}:{row[2]}"


def chunk_ranges(min_v: int, max_v: int, chunk_size: int) -> List[Tuple[int, int]]:
    ranges = []
    cur = min_v
    while cur <= max_v:
        end = cur + chunk_size - 1
        if end > max_v:
            end = max_v
        ranges.append((cur, end))
        cur = end + 1
    return ranges


def main():
    ap = argparse.ArgumentParser(description="MySQL 마이그레이션 검증 스크립트")
    ap.add_argument("--src-host", required=True)
    ap.add_argument("--src-port", type=int, default=3306)
    ap.add_argument("--src-user", required=True)
    ap.add_argument("--src-pass", required=True)
    ap.add_argument("--src-db", required=True)

    ap.add_argument("--dst-host", required=True)
    ap.add_argument("--dst-port", type=int, default=3306)
    ap.add_argument("--dst-user", required=True)
    ap.add_argument("--dst-pass", required=True)
    ap.add_argument("--dst-db", required=True)

    ap.add_argument("--out", default="migration_report.json")

    ap.add_argument(
        "--hash-mode",
        choices=["off", "sample", "pk-range"],
        default="off",
        help="off: 해시 비교 없음, sample: 일부 샘플 해시, pk-range: PK 범위 청크 해시",
    )
    ap.add_argument("--sample-limit", type=int, default=1000)
    ap.add_argument(
        "--hash-pk",
        default=None,
        help="pk-range 해시용 PK 컬럼명. 미지정 시 단일 PK를 자동 감지 시도.",
    )
    ap.add_argument("--hash-chunk-size", type=int, default=200000)

    ap.add_argument(
        "--tables",
        default=None,
        help="검증할 테이블 목록(콤마 구분). 미지정 시 src-db의 모든 base table 검증.",
    )
    args = ap.parse_args()

    src = DbCfg(args.src_host, args.src_port, args.src_user, args.src_pass, args.src_db)
    dst = DbCfg(args.dst_host, args.dst_port, args.dst_user, args.dst_pass, args.dst_db)

    report = {
        "메타": {
            "시작시각": time.strftime("%Y-%m-%d %H:%M:%S"),
            "원본": {"호스트": src.host, "포트": src.port, "DB": src.database, "사용자": src.user},
            "대상": {"호스트": dst.host, "포트": dst.port, "DB": dst.database, "사용자": dst.user},
            "해시_모드": args.hash_mode,
        },
        "요약": {},
        "테이블": {},
        "오류": [],
    }

    try:
        src_conn = connect(src)
        dst_conn = connect(dst)
    except Exception as e:
        print(f"[치명] DB 연결 실패: {e}", file=sys.stderr)
        sys.exit(2)

    try:
        if args.tables:
            tables = [t.strip() for t in args.tables.split(",") if t.strip()]
        else:
            tables = list_tables(src_conn, src.database)

        dst_tables = set(list_tables(dst_conn, dst.database))
        missing_in_dst = [t for t in tables if t not in dst_tables]

        report["요약"]["원본_테이블수"] = len(tables)
        report["요약"]["대상_테이블수"] = len(dst_tables)
        report["요약"]["대상_누락_테이블"] = missing_in_dst

        ddl_mismatch = 0
        idx_mismatch = 0
        rowcount_mismatch = 0
        hash_mismatch = 0

        for t in tables:
            entry = {
                "대상_존재": t in dst_tables,
                "DDL": {"일치": None, "원본_길이": None, "대상_길이": None},
                "인덱스": {"일치": None, "원본": None, "대상": None},
                "행수": {"일치": None, "원본": None, "대상": None},
                "해시": {"모드": args.hash_mode, "일치": None, "상세": None},
                "메모": [],
            }

            if t not in dst_tables:
                entry["DDL"]["일치"] = False
                entry["인덱스"]["일치"] = False
                entry["행수"]["일치"] = False
                entry["해시"]["일치"] = False
                entry["메모"].append("대상_테이블_없음")
                report["테이블"][t] = entry
                continue

            # DDL 비교
            try:
                src_ddl = normalize_ddl(show_create_table(src_conn, t))
                dst_ddl = normalize_ddl(show_create_table(dst_conn, t))
                entry["DDL"]["원본_길이"] = len(src_ddl)
                entry["DDL"]["대상_길이"] = len(dst_ddl)
                entry["DDL"]["일치"] = (src_ddl == dst_ddl)
                if not entry["DDL"]["일치"]:
                    ddl_mismatch += 1
            except Exception as e:
                entry["DDL"]["일치"] = None
                report["오류"].append({"테이블": t, "단계": "DDL", "오류": str(e)})

            # 인덱스 비교
            try:
                src_idx = get_indexes(src_conn, src.database, t)
                dst_idx = get_indexes(dst_conn, dst.database, t)
                entry["인덱스"]["원본"] = src_idx
                entry["인덱스"]["대상"] = dst_idx
                entry["인덱스"]["일치"] = (src_idx == dst_idx)
                if not entry["인덱스"]["일치"]:
                    idx_mismatch += 1
            except Exception as e:
                entry["인덱스"]["일치"] = None
                report["오류"].append({"테이블": t, "단계": "인덱스", "오류": str(e)})

            # 행 수 비교
            try:
                src_cnt = count_rows_exact(src_conn, t)
                dst_cnt = count_rows_exact(dst_conn, t)
                entry["행수"]["원본"] = src_cnt
                entry["행수"]["대상"] = dst_cnt
                entry["행수"]["일치"] = (src_cnt == dst_cnt)
                if not entry["행수"]["일치"]:
                    rowcount_mismatch += 1
            except Exception as e:
                entry["행수"]["일치"] = None
                report["오류"].append({"테이블": t, "단계": "행수", "오류": str(e)})

            # 선택: 해시 비교
            if args.hash_mode != "off":
                try:
                    if args.hash_mode == "sample":
                        src_h = hash_sample(src_conn, t, limit=args.sample_limit)
                        dst_h = hash_sample(dst_conn, t, limit=args.sample_limit)
                        entry["해시"]["상세"] = {"원본": src_h, "대상": dst_h, "샘플_제한": args.sample_limit}
                        entry["해시"]["일치"] = (src_h == dst_h)
                        if not entry["해시"]["일치"]:
                            hash_mismatch += 1

                    elif args.hash_mode == "pk-range":
                        pk = args.hash_pk or detect_primary_key(src_conn, src.database, t)
                        if not pk:
                            entry["해시"]["일치"] = None
                            entry["메모"].append("pk_range_스킵(단일_PK_없음)")
                        else:
                            smin, smax = pk_min_max(src_conn, t, pk)
                            dmin, dmax = pk_min_max(dst_conn, t, pk)
                            if smin is None or smax is None:
                                # 빈 테이블
                                entry["해시"]["상세"] = {"PK": pk, "빈_테이블": True}
                                entry["해시"]["일치"] = True
                            else:
                                # PK 범위가 다르면 즉시 불일치 처리
                                if smin != dmin or smax != dmax:
                                    entry["해시"]["상세"] = {
                                        "PK": pk,
                                        "원본_최소": smin,
                                        "원본_최대": smax,
                                        "대상_최소": dmin,
                                        "대상_최대": dmax,
                                    }
                                    entry["해시"]["일치"] = False
                                    hash_mismatch += 1
                                else:
                                    ranges = chunk_ranges(int(smin), int(smax), args.hash_chunk_size)
                                    mismatches = []
                                    for (a, b) in ranges:
                                        sh = hash_pk_range(src_conn, t, pk, a, b)
                                        dh = hash_pk_range(dst_conn, t, pk, a, b)
                                        if sh != dh:
                                            mismatches.append({"범위": [a, b], "원본": sh, "대상": dh})
                                    entry["해시"]["상세"] = {
                                        "PK": pk,
                                        "청크_크기": args.hash_chunk_size,
                                        "범위_개수": len(ranges),
                                        "불일치": mismatches,
                                    }
                                    entry["해시"]["일치"] = (len(mismatches) == 0)
                                    if not entry["해시"]["일치"]:
                                        hash_mismatch += 1
                except Exception as e:
                    entry["해시"]["일치"] = None
                    report["오류"].append({"테이블": t, "단계": "해시", "오류": str(e)})

            report["테이블"][t] = entry

        report["요약"].update({
            "DDL_불일치_테이블": ddl_mismatch,
            "인덱스_불일치_테이블": idx_mismatch,
            "행수_불일치_테이블": rowcount_mismatch,
            "해시_불일치_테이블": hash_mismatch if args.hash_mode != "off" else None,
        })
        report["메타"]["종료시각"] = time.strftime("%Y-%m-%d %H:%M:%S")

        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print(f"[완료] 리포트 저장: {args.out}")
        print(json.dumps(report["요약"], ensure_ascii=False, indent=2))

        # CI 친화적 종료 코드:
        # 0 = 이상 없음, 1 = 불일치 존재, 2 = 치명적 오류
        mismatches_exist = (
            ddl_mismatch > 0
            or idx_mismatch > 0
            or rowcount_mismatch > 0
            or (
                args.hash_mode != "off"
                and report["요약"]["해시_불일치_테이블"]
                and report["요약"]["해시_불일치_테이블"] > 0
            )
            or len(report["요약"]["대상_누락_테이블"]) > 0
        )
        sys.exit(1 if mismatches_exist else 0)

    finally:
        try:
            src_conn.close()
        except Exception:
            pass
        try:
            dst_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
