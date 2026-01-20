# MySQL 마이그레이션 검증기

소스 DB와 대상 DB의 스키마/인덱스/행 수를 비교하고, 선택적으로 데이터 해시를 비교합니다.
결과는 JSON 리포트로 저장되며, CI에서 불일치 여부를 exit code로 판단할 수 있습니다.

## 실행 방법

1. 의존성 설치

```bash
pip install mysql-connector-python
```

2. 기본 실행

```bash
python3 verify_mysql_migration.py \
  --src-host 127.0.0.1 --src-port 3306 --src-user root --src-pass 'SRC_PASS' --src-db source_db \
  --dst-host 127.0.0.1 --dst-port 3306 --dst-user root --dst-pass 'DST_PASS' --dst-db target_db
```

3. 특정 테이블만 검증

```bash
python3 verify_mysql_migration.py \
  --src-host 127.0.0.1 --src-port 3306 --src-user root --src-pass 'SRC_PASS' --src-db source_db \
  --dst-host 127.0.0.1 --dst-port 3306 --dst-user root --dst-pass 'DST_PASS' --dst-db target_db \
  --tables users,orders,order_items
```

4. 샘플 해시 비교

```bash
python3 verify_mysql_migration.py \
  --src-host 127.0.0.1 --src-port 3306 --src-user root --src-pass 'SRC_PASS' --src-db source_db \
  --dst-host 127.0.0.1 --dst-port 3306 --dst-user root --dst-pass 'DST_PASS' --dst-db target_db \
  --hash-mode sample --sample-limit 2000
```

5. PK 범위 해시 비교

```bash
python3 verify_mysql_migration.py \
  --src-host 127.0.0.1 --src-port 3306 --src-user root --src-pass 'SRC_PASS' --src-db source_db \
  --dst-host 127.0.0.1 --dst-port 3306 --dst-user root --dst-pass 'DST_PASS' --dst-db target_db \
  --hash-mode pk-range --hash-pk id --hash-chunk-size 100000
```

## 리포트 예시

- 기본 출력: `migration_report.json`
- 콘솔에 요약 결과가 출력됩니다.
- 불일치가 있으면 exit code `1`을 반환합니다.

## 주의 사항

- `sample` 해시는 `ORDER BY 1`(첫 컬럼 기준 정렬)에 의존하므로, 컬럼 구조에 따라 안정성이 떨어질 수 있습니다.
- `GROUP_CONCAT` 길이 제한으로 인해 큰 테이블에서는 해시가 잘릴 수 있습니다.
- `pk-range`는 단일 PK만 지원합니다.
