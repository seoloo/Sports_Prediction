# 코드 가이드

코드를 처음 읽거나 면접에서 구현 흐름을 설명할 때 보는 안내서입니다. 각 함수의 자세한 설명은 소스의 docstring(한국어)에 있습니다.

## 모듈 지도

| 파일 | 역할 | 대표 함수 |
|---|---|---|
| [infra/docker-compose.yml](../infra/docker-compose.yml) | PostgreSQL·SeaweedFS(S3) 컨테이너, 127.0.0.1 포트 | — |
| [pipelines/leagues.py](../pipelines/leagues.py) | 다섯 리그의 소스별 이름(openfootball·football-data.org·The Odds API·co.uk)과 모델이 있는 리그(5a) | `LEAGUES`, `MODELED` |
| [pipelines/config.py](../pipelines/config.py) | `.env` 읽기, 무작위 비밀번호로 `.env` 만들기 | `load_settings`, `init_env` |
| [pipelines/db.py](../pipelines/db.py) | Postgres 연결, `sql/NNN_*.sql` 순서 적용 | `connect`, `migrate` |
| [pipelines/sql/001_ingest_silver.sql](../pipelines/sql/001_ingest_silver.sql) | `ingest`·`silver` 스키마, 불변 트리거, 기준 시각 함수 | `silver.match_revisions_known_at` |
| [pipelines/object_store.py](../pipelines/object_store.py) | S3 Bronze 저장(내용 해시 경로, 없을 때만 업로드) | `put_if_absent`, `read_object` |
| [pipelines/ingest/openfootball.py](../pipelines/ingest/openfootball.py) | openfootball 수집: 정규화(순수 함수) → Bronze → Silver | `normalize_document`, `write_silver`, `ingest` |
| [pipelines/ingest/football_data_co_uk.py](../pipelines/ingest/football_data_co_uk.py) | 직접 내려받은 배당 CSV: 열 이름 기반 파싱 → 대조표로 경기 연결 → COPY + 집합 연산으로 바뀐 배당·경기 기록(3g)만 버전 추가 | `parse_csv`, `extract_stats`, `link_fixtures`, `write_odds`, `write_stats` |
| [pipelines/mappings/team_aliases.csv](../pipelines/mappings/team_aliases.csv) | 검토한 팀 이름 대조표(같은 날짜 경기 근거) | — |
| [pipelines/ingest/the_odds_api.py](../pipelines/ingest/the_odds_api.py) | The Odds API 수집: 스냅샷 직전에만 리그별 배당(크레딧 예산), Bronze 원본, 값이 바뀐 배당만 버전, 팀 대조표 후보(5b) | `run_due`, `due_offsets`, `quotes`, `write_odds`, `suggest_aliases` |
| [pipelines/ingest/balldontlie.py](../pipelines/ingest/balldontlie.py) | NBA 수집(5d-a): cursor로 시즌 전체 → 쪽 단위 Bronze → 정규화(순수 함수) → `silver.games`·`game_revisions`(바뀐 경기만) | `fetch_season`, `normalize_game`, `write_silver`, `ingest`, `current_season` |
| [pipelines/ingest/the_odds_api_nba.py](../pipelines/ingest/the_odds_api_nba.py) | NBA 배당(5d-b): 동부 날짜별 첫 경기 1시간 전 하루 1번, 시작 ±3시간 경기 연결, 바뀐 배당만 `silver.game_odds_revisions` 버전. 핸디캡·언더오버는 요청할 때만(`--lines`) | `run_due`, `due_reasons`, `quotes`, `find_game`, `write_odds`, `fetch_lines`, `suggest_aliases` |
| [pipelines/aliases.py](../pipelines/aliases.py) | 검토한 팀 대조표 읽기·반영(소스 공통) | `load_alias_file`, `sync_aliases` |
| [pipelines/ingest/football_data_org.py](../pipelines/ingest/football_data_org.py) | API 수집기: 요청 한도·429 처리, Bronze 보존, 바뀐 경우에만 이벤트 발행, 조회 간격 자동 조절 | `fetch_json`, `poll_once`, `next_poll_interval` |
| [pipelines/ingest/rate_limit.py](../pipelines/ingest/rate_limit.py) | 의존성 없는 공용 요청 제한기(최근 60초 요청 수). football-data.org 10회, balldontlie 5회 | `RateLimiter` |
| [pipelines/streaming/events.py](../pipelines/streaming/events.py) | 이벤트 겉봉투(내용 해시 event_id, published_at·observed_at) | `build_event`, `decode` |
| [pipelines/streaming/kafka_io.py](../pipelines/streaming/kafka_io.py) | 토픽 생성, 멱등 생산자, 수동 커밋 소비자 | `ensure_topics`, `producer`, `consumer` |
| [pipelines/streaming/match_consumer.py](../pipelines/streaming/match_consumer.py) | 소비자: 중복·늦은 이벤트 처리, DB 커밋 후 오프셋 커밋 | `handle_match_event`, `run` |
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
| [pipelines/modeling/nba_models.py](../pipelines/modeling/nba_models.py) | NBA 2지선다 모델(5d-c): 단순 비율, 점수 차 등급(가중 ridge + Φ), Elo(이어서 갱신) | `Game`, `MarginRatingModel`, `EloModel` |
| [pipelines/modeling/nba_backtest.py](../pipelines/modeling/nba_backtest.py) | NBA 백테스트·평가(5d-c): 버블·연전 표시, 주 블록 walk-forward(+4시간), 격자 선택, 보정·부트스트랩·보고서 | `load_games`, `walk_forward`, `run`, `evaluate`, `render_markdown` |
| [pipelines/modeling/nba_prospective.py](../pipelines/modeling/nba_prospective.py) | NBA 경기 전 예측(5d-d): 그 순간 받아 둔 결과로 활성 모델 학습, 확률은 평가용·WITHHELD | `NbaPredictor`, `record_for_snapshot` |
| [pipelines/nba_snapshots.py](../pipelines/nba_snapshots.py) | NBA 경기 전 스냅샷(5d-d): 24·3·1시간 전 입력(최근 5경기·연전·배당), 같은 시각의 예측 | `run`, `build_inputs`, `known_games` |
| [pipelines/modeling/market.py](../pipelines/modeling/market.py) | 시장 기준 확률(마진 제거, 출처 우선순위 co.uk 평균 → Pinnacle → 북메이커 3곳 이상 평균). 경기 전 예측 채점의 비교 기준 | `football_market_fair`, `nba_market_fair`, `nba_odds_at` |
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

## 이름 규칙

- `…_at`: 시각(UTC). `published_at` 출처 발표 시각, `received_at` 실제로 받은 시각, `as_of` 기준 시각
- `…_known_at(as_of)`: "그 시각에 알려져 있던" 최신 버전을 돌려주는 SQL 함수
- `…_revisions`: 추가만 하는 버전 테이블(수정·삭제는 트리거가 막음)

## 흐름 1: 스트리밍 이벤트 하나가 Silver에 들어갈 때

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
