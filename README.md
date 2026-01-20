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
  --src-host 127.0.0.1 --src-port 3306 --src-user root --src-pass '원본비밀번호' --src-db 원본_DB명 \
  --dst-host 127.0.0.1 --dst-port 3306 --dst-user root --dst-pass '대상비밀번호' --dst-db 대상_DB명
```

3. 특정 테이블만 검증

```bash
python3 verify_mysql_migration.py \
  --src-host 127.0.0.1 --src-port 3306 --src-user root --src-pass '원본비밀번호' --src-db 원본_DB명 \
  --dst-host 127.0.0.1 --dst-port 3306 --dst-user root --dst-pass '대상비밀번호' --dst-db 대상_DB명 \
  --tables users,orders,order_items
```

4. 샘플 해시 비교

```bash
python3 verify_mysql_migration.py \
  --src-host 127.0.0.1 --src-port 3306 --src-user root --src-pass '원본비밀번호' --src-db 원본_DB명 \
  --dst-host 127.0.0.1 --dst-port 3306 --dst-user root --dst-pass '대상비밀번호' --dst-db 대상_DB명 \
  --hash-mode sample --sample-limit 2000
```

5. PK 범위 해시 비교

```bash
python3 verify_mysql_migration.py \
  --src-host 127.0.0.1 --src-port 3306 --src-user root --src-pass '원본비밀번호' --src-db 원본_DB명 \
  --dst-host 127.0.0.1 --dst-port 3306 --dst-user root --dst-pass '대상비밀번호' --dst-db 대상_DB명 \
  --hash-mode pk-range --hash-pk id --hash-chunk-size 100000
```

## 변수 설명

- `--src-db`: 기존(원본) DB 이름. 예: `원본_DB명`
- `--dst-db`: 신규(대상) DB 이름. 예: `대상_DB명`
- `--src-user`: 기존(원본) DB 사용자명. 예: `원본_DB_사용자`
- `--dst-user`: 신규(대상) DB 사용자명. 예: `대상_DB_사용자`
- `--src-pass`: 기존(원본) DB 비밀번호. 예: `'원본비밀번호'`
- `--dst-pass`: 신규(대상) DB 비밀번호. 예: `'대상비밀번호'`

## VPN 환경 실행 예시 (Private 도메인)

```bash
python3 verify_mysql_migration.py \
  --src-host db-src.private.company.local --src-port 3306 --src-user user --src-pass '원본비밀번호' --src-db 원본_DB명 \
  --dst-host db-dst.private.company.local --dst-port 3306 --dst-user user --dst-pass '대상비밀번호' --dst-db 대상_DB명
```

## 리포트 예시

- 기본 출력: `migration_report.json`
- 콘솔에 요약 결과가 출력됩니다.
- 불일치가 있으면 exit code `1`을 반환합니다.

## 주의 사항

- `sample` 해시는 PK/UNIQUE 기반 정렬을 우선 사용하지만, 키가 없으면 첫 컬럼 기준 정렬을 사용합니다.
- 해시 비교는 CRC32 집계이므로 아주 드문 충돌 가능성이 있습니다.
- `pk-range`는 단일 PK만 지원합니다.
