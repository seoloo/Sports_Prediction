# 코드 가이드

코드를 처음 읽거나 면접에서 구현 흐름을 설명할 때 보는 안내서입니다. 각 함수의 자세한 설명은 소스의 docstring(한국어)에 있습니다.

## 모듈 지도

| 파일 | 역할 | 대표 함수 |
|---|---|---|
| [app.py](../app.py) | Streamlit 진입점, 기본 메뉴 5개(전체 경기·CSV 가져오기·직접 등록·데이터 소스·가져오기 이력) | `all_events_page`, `csv_import_page` |
| [src/history.py](../src/history.py) | 변경 이력(버전 추가), DB 버전 이전, 기준 시각 스냅샷 | `latest_revision_known_at`, `revise_market`, `build_replay_snapshot` |
| [src/evidence.py](../src/evidence.py) | 기록 정정, 경기 결과, 목적별 사용권, 공식 결과 대조 | `records_known_at`, `require_permission_at`, `compare_official_result` |
| [src/bulk_import.py](../src/bulk_import.py) | 결과·기록 정정 CSV 일괄 입력(원본 정정 식별자, 버전 충돌 검사) | `normalize_bulk_row`, `import_bulk_csv` |
| [src/features.py](../src/features.py) | 종목별 특징 계약, 입력된 요약값으로 특징 생성 | `feature_contract_definitions`, `generate_features` |
| [src/history_features.py](../src/history_features.py) | 과거 결과에서 기준 시각 특징 자동 집계 | `aggregate_result_history` |
| [src/demo.py](../src/demo.py) | 가상 예시 데이터 | `demo_csv_rows`, `load_demo_data` |
| [src/evidence_pages.py](../src/evidence_pages.py) | 화면: 목적별 사용권, 기록 정정, 결과·공식 대조, 모델 입력·특징 | `results_page`, `features_page` |

화면 파일(`app.py`, `*_pages.py`)은 입력을 받아 위 모듈 함수를 부르고 결과를 표로 보여주기만 합니다. 검사·저장 규칙은 화면 밖에 있습니다.

### 데이터 플랫폼 (pipelines/)

| 파일 | 역할 | 대표 함수 |
|---|---|---|
| [infra/docker-compose.yml](../infra/docker-compose.yml) | PostgreSQL·SeaweedFS(S3) 컨테이너, 127.0.0.1 포트 | — |
| [pipelines/config.py](../pipelines/config.py) | `.env` 읽기, 무작위 비밀번호로 `.env` 만들기 | `load_settings`, `init_env` |
| [pipelines/db.py](../pipelines/db.py) | Postgres 연결, `sql/NNN_*.sql` 순서 적용 | `connect`, `migrate` |
| [pipelines/sql/001_ingest_silver.sql](../pipelines/sql/001_ingest_silver.sql) | `ingest`·`silver` 스키마, 불변 트리거, 기준 시각 함수 | `silver.match_revisions_known_at` |
| [pipelines/object_store.py](../pipelines/object_store.py) | S3 Bronze 저장(내용 해시 경로, 없을 때만 업로드) | `put_if_absent`, `read_object` |
| [pipelines/ingest/openfootball.py](../pipelines/ingest/openfootball.py) | openfootball 수집: 정규화(순수 함수) → Bronze → Silver | `normalize_document`, `write_silver`, `ingest` |
| [pipelines/ingest/football_data_co_uk.py](../pipelines/ingest/football_data_co_uk.py) | 직접 내려받은 배당 CSV: 열 이름 기반 파싱 → 대조표로 경기 연결 → COPY + 집합 연산으로 바뀐 배당·경기 기록(3g)만 버전 추가 | `parse_csv`, `extract_stats`, `link_fixtures`, `write_odds`, `write_stats` |
| [pipelines/mappings/team_aliases.csv](../pipelines/mappings/team_aliases.csv) | 검토한 팀 이름 대조표(같은 날짜 경기 근거) | — |
| [pipelines/aliases.py](../pipelines/aliases.py) | 검토한 팀 대조표 읽기·반영(소스 공통) | `load_alias_file`, `sync_aliases` |
| [pipelines/ingest/football_data_org.py](../pipelines/ingest/football_data_org.py) | API 수집기: 요청 한도·429 처리, Bronze 보존, 바뀐 경우에만 이벤트 발행, 조회 간격 자동 조절 | `RateLimiter`, `fetch_json`, `poll_once`, `next_poll_interval` |
| [pipelines/streaming/events.py](../pipelines/streaming/events.py) | 이벤트 겉봉투(내용 해시 event_id, published_at·observed_at) | `build_event`, `decode` |
| [pipelines/streaming/kafka_io.py](../pipelines/streaming/kafka_io.py) | 토픽 생성, 멱등 생산자, 수동 커밋 소비자 | `ensure_topics`, `producer`, `consumer` |
| [pipelines/streaming/match_consumer.py](../pipelines/streaming/match_consumer.py) | 소비자: 중복·늦은 이벤트 처리, DB 커밋 후 오프셋 커밋, 토픽별 처리 함수 | `handle_match_event`, `run` |
| [pipelines/streaming/replay.py](../pipelines/streaming/replay.py) | 과거 시즌 재생 + 장애 주입 + 최종 상태 검증 | `inject_faults`, `replay_season` |
| [pipelines/snapshots.py](../pipelines/snapshots.py) | 경기 시작 24·3·1시간 전 입력 스냅샷(놓친 시점은 MISSED) | `decide`, `build_inputs`, `run` |
| [pipelines/lake/export.py](../pipelines/lake/export.py) | Gold → Parquet(lake 버킷, 시즌 파티션, 내용 해시 스냅샷, 정답 dbt 결과 포함)(4a) | `export`, `snapshot_id`, `to_parquet` |
| [pipelines/lake/spark_features.py](../pipelines/lake/spark_features.py) | Spark로 시간순 특징 재계산(조인 + row_number), 결과 업로드(4a) | `compute_features`, `run` |
| [pipelines/lake/reconcile.py](../pipelines/lake/reconcile.py) | Spark 결과 ↔ dbt 결과 행·열 대조(pyarrow)(4a) | `reconcile`, `same` |
| [pipelines/lake/run.py](../pipelines/lake/run.py) | export → spark → reconcile 실행(실패 시 종료 코드 1), Airflow용 별도 가상환경 실행 | `main`, `run_isolated` |
| [pipelines/notify/telegram.py](../pipelines/notify/telegram.py) | 텔레그램 Bot API 호출(토큰 지움, 4096자 분할, 429 대기), 대화방 ID 확인·시험 명령(4b) | `send`, `call`, `chat_ids` |
| [pipelines/notify/alerts.py](../pipelines/notify/alerts.py) | 경기 전·주간·실패 알림 문구, 보낸 기록으로 중복 방지, Airflow 실패 콜백(4b) | `notify_prematch`, `notify_weekly`, `airflow_failure_callback` |
| [pipelines/dbt_runner.py](../pipelines/dbt_runner.py) | `.env` 접속 정보를 넘겨 별도 가상환경의 dbt 실행 | `run_dbt` |
| [pipelines/schedule.py](../pipelines/schedule.py) | 날짜 → 수집할 시즌 이름(순수 함수) | `seasons_for` |
| [pipelines/modeling/dataset.py](../pipelines/modeling/dataset.py) | 시간순 데이터셋 읽기, Python 독립 재계산으로 누수 검사(3a) | `load_dataset`, `verify_point_in_time` |
| [pipelines/modeling/models.py](../pipelines/modeling/models.py) | 기준 모델: 단순 비율, Poisson(ridge IRLS·시간 감쇠), Dixon-Coles(3b), 승격 첫 시즌 효과·확률 보정(3e), 유효 슈팅·퇴장·휴식 옵션과 시장 결합(3g) | `TrainingMatch`, `PoissonModel.fit`, `MarketBlendModel`, `blend`, `model_from_params` |
| [pipelines/modeling/backtest.py](../pipelines/modeling/backtest.py) | 주 단위 재학습 백테스트, 검증 시즌에서 설정 선택, 시험 시즌 평가, 실행·예측 기록(3b) | `walk_forward`, `summarize`, `store_run`, `run` |
| [pipelines/modeling/prospective.py](../pipelines/modeling/prospective.py) | 스냅샷 시점의 모델 예측(실제 수신 자료만, 검증 전 WITHHELD), 경기 후 채점, 주간 보고서(3d) | `active_model`, `Predictor`, `record_for_snapshot`, `run_weekly` |
| [pipelines/modeling/evaluation.py](../pipelines/modeling/evaluation.py) | 저장된 시험 예측 평가: 블록 부트스트랩, 확률 보정, 약한 곳, 검증 기준, 보고서 저장·Markdown 생성(3c) | `block_bootstrap`, `calibration`, `segments`, `gate`, `render_markdown` |
| [dbt/models/](../dbt/models/) | `staging.stg_matches_known` → `gold.team_match_results` → `gold.league_standings`, `staging.stg_odds_known` → `gold.match_odds_1x2` | — |
| [dbt/tests/](../dbt/tests/) | 순위표 정합성·끝난 경기당 팀 행 2개 검사 | — |
| [dags/openfootball_epl_daily.py](../dags/openfootball_epl_daily.py) | 매일 스키마 확인 → 시즌별 수집(동적 태스크) → 배당 CSV 수집 → dbt build → Spark 재계산·대조(4a) | — |
| [dags/prematch_snapshots.py](../dags/prematch_snapshots.py) | 30분마다 경기 전 스냅샷 + 모델 예측 | — |
| [dags/weekly_model_report.py](../dags/weekly_model_report.py) | 화요일 06:00 UTC 지난주 예측 채점·주간 보고서 | — |
| [infra/airflow/Dockerfile](../infra/airflow/Dockerfile) | Airflow 3.3.2 이미지 + 파이프라인 라이브러리 + dbt 전용 가상환경 | — |
| [.github/workflows/ci.yml](../.github/workflows/ci.yml) | CI: Postgres·SeaweedFS를 띄워 전체 테스트와 종단 실행 | — |

## 흐름 0: openfootball 한 시즌을 수집할 때

```text
ingest(season, league)
 ├ ingest.runs에 RUNNING 기록
 ├ 원본 내려받기 → SHA-256 → S3 Bronze에 없을 때만 업로드
 ├ normalize_document(순수 함수)
 │   ├ score: 객체 {ft, ht} / 배열 [h, a] / 없음 → 전반 점수 모르면 None(0으로 채우지 않음)
 │   ├ 현지 시각 + 리그 시간대 → UTC, 존재하지 않는 서머타임 시각은 거부
 │   └ 실패 레코드 → QualityIssue(이유 코드)
 ├ 한 트랜잭션: raw_files(같은 내용이면 기존 행) + fetches + write_silver + quality_issues
 │   └ write_silver: 경기별 직전 버전과 내용 해시가 다를 때만 다음 버전 추가
 └ ingest.runs를 SUCCEEDED(통계) 또는 FAILED(오류)로 갱신
```

## 흐름 1: CSV 한 파일을 저장할 때

```text
화면에서 파일 선택
 └ Catalog.preview_csv(kind, payload)          # 미리보기. DB에 쓰지 않음
    └ Catalog._check_csv                       # 크기 → 인코딩 → 열 이름 → 행 검사 → 중복·충돌
       └ Catalog._validate_and_normalize_row   # 소스 승인, 시간대, 참가자 이름, 마감 ≤ 시작
 └ [저장 버튼] Catalog.import_csv(kind, payload, filename)
    ├ BEGIN IMMEDIATE (쓰기 잠금)
    ├ _check_csv 다시 실행                     # 미리보기 이후 바뀐 상태 반영
    ├ 같은 SHA-256 파일이면 중단(duplicate_file)
    ├ 행 INSERT + append_initial_revision      # 1번 버전을 이력 테이블에 남김
    └ raw_imports(원본 바이트·해시) + audit_log 저장
결과·기록 정정 CSV는 bulk_import.preview_bulk_csv / import_bulk_csv로 갈라집니다.
```

## 흐름 2: 기준 시각 스냅샷을 만들 때

```text
 ├ as_of가 미래·경기 시작 이후·마감 이후면 거부
 │   └ latest_revision_known_at                # published_at, received_at 모두 ≤ as_of
 ├ 규칙 확인(load_rule_version, find_rule_mismatches) → 아니면 UNKNOWN_RULE
 ├ records_known_at → 기록을 포함/제외로 분류(제외 이유 기록)
 ├ [특징 계약이 있으면] generate_features
 │   └ [과거 결과 집계 계약] aggregate_result_history
 ├ data_hash = 입력 내용만으로 SHA-256         # 같은 입력이면 같은 해시
 └ status는 항상 WITHHELD(모델 미검증), reasons에 보류 이유
```

## 이름 규칙

- `…_at`: 시각 문자열(UTC ISO 8601). `published_at` 발표 시각, `received_at` 실제 입력 시각, `as_of` 기준 시각
- `…_known_at`, `…_version_at`: "그 시각에 알려져 있던" 버전을 찾는 함수
- `require_…`: 조건을 확인하고, 맞지 않으면 ValueError를 내는 함수
- `validate_…`: 값을 검사해 저장 형태로 돌려주는 함수(DB에 쓰지 않음)
- `append_…_revision`: 새 버전 행을 추가하는 함수(트랜잭션은 호출한 쪽이 관리)
- `db`: 항상 `Catalog.connect()`가 연 sqlite3 연결

## 바꾸면 안 되는 이름

DB 테이블·열 이름, 스냅샷 JSON 키(`observations`, `exclusions`, `excluded_reasons`, `model_features` 등), 특징 계약 내용, CSV 열 이름, Streamlit 위젯 key는 바꾸지 않았습니다. 이 값들이 바뀌면 기존 DB를 읽지 못하거나, 같은 입력의 스냅샷 해시가 달라지거나, 화면 상태가 초기화됩니다.

## 2026-10-20 리팩토링 이름 대조표

동작은 바꾸지 않고 이름과 주석만 바꿨습니다. 고정 시계·고정 ID로 같은 시나리오를 실행해, 리팩토링 전후 DB 모든 행·반환값·스냅샷 해시가 같음을 확인했습니다.

| 이전 | 새 이름 |
|---|---|
| `src/bulk.py`, `src/next_pages.py` | `src/bulk_import.py`, `src/history_pages.py` |
| `SPORTS`, `HEADERS`, `MAX_BYTES`, `MAX_ROWS`, `_WRITE_LOCK` | `SPORT_LABELS`, `CSV_HEADERS`, `MAX_CSV_BYTES`, `MAX_CSV_ROWS`, `DB_WRITE_LOCK` |
| `SCHEMA` (catalog/history/evidence/bulk) | `CATALOG_SCHEMA`, `HISTORY_SCHEMA`, `EVIDENCE_SCHEMA`, `BULK_IMPORT_SCHEMA` |
| `timestamp`, `korean_time`, `decimal_text`, `csv_bytes` | `to_utc_timestamp`, `format_kst`, `normalize_decimal`, `build_csv_bytes` |
| `Preview`, `.ok`, `.skipped` | `CsvPreview`, `.can_save`, `.skipped_count` |
| `Catalog.demo` | `Catalog.is_demo` |
| `canonical`, `before`, `known_revision`, `audit` | `to_canonical_json`, `is_at_or_before`, `latest_revision_known_at`, `write_audit_log` |
| `initialize_history`, `append_initial`, `source_check` | `initialize_history_schema`, `append_initial_revision`, `require_approved_source` |
| `rules`, `rule_from_db`, `event_payload`, `market_payload` | `list_rule_versions`, `load_rule_version`, `event_version_at`, `market_version_at` |
| `build_snapshot`, `snapshots` | `build_replay_snapshot`, `list_saved_snapshots` |
| `append_record_initial`, `checked_time`, `http_url`, `permission_at` | `append_initial_record_revision`, `validate_publication`, `validate_http_url`, `require_permission_at` |
| `append_record`, `append_result`, `resolved_records`, `result_at` | `append_record_revision`, `append_result_revision`, `records_known_at`, `result_known_at` |
| `save_official`, `compare_result`, `IMMUTABLE` | `save_official_result`, `compare_official_result`, `IMMUTABLE_EVIDENCE_TABLES` |
| `contracts`, `aggregate_results` | `feature_contract_definitions`, `aggregate_result_history` |
| `preset_rules`, `compatibility`, `PRESETS_PATH` | `load_preset_rules`, `find_rule_mismatches`, `RULE_PRESET_PATH` |
| `unique_json_pairs`, `normalize`, `preview`, `import_batch` (bulk) | `reject_duplicate_json_keys`, `normalize_bulk_row`, `preview_bulk_csv`, `import_bulk_csv` |
| `result_examples`, `correction_template` | `demo_result_rows`, `record_correction_template_rows` |
| `sample_rows`, `load_demo` | `demo_csv_rows`, `load_demo_data` |
| `catalog_page`, `import_page`, `manual_page`, `history_page` (app.py) | `all_events_page`, `csv_import_page`, `manual_entry_page`, `import_history_page` |

## 흐름 3: 스트리밍 이벤트 하나가 Silver에 들어갈 때

```text
fdorg-poller: poll_once
 ├ RateLimiter.wait → fetch_json(429면 초기화 시간만큼 대기 후 재시도)
 ├ 응답 원본 → Bronze(S3, 내용 해시 경로) + ingest.raw_files·fetches
 └ 내용이 바뀐 경우에만 경기별 build_event → Kafka(키 = 경기 ID, 같은 경기는 같은 파티션)
stream-consumer: run
 ├ 묶음(최대 500건)을 한 DB 트랜잭션으로 처리
 │   └ handle_match_event: 형식 → 팀 대조 → 경기 연결(없으면 생성)
 │       → 내용 같음/이미 반영한 event_id = DUPLICATE
 │       → 최신보다 이른 published_at = STALE_EVENT(stream_rejections)
 │       → 그 밖에는 live_match_revisions 다음 버전
 └ DB 커밋 → Kafka 오프셋 커밋(중간에 죽으면 다시 받아도 결과는 같다)
```
