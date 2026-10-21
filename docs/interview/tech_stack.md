# 사용한 기술 스택

작성: 2026-10-21(4a 시점). 버전은 저장소에 고정된 값입니다(`requirements*.txt`, `infra/docker-compose.yml`, `infra/*/Dockerfile`).

## 한눈에 보기

| 층 | 기술 | 버전 | 상태 |
|---|---|---|---|
| 언어 | Python | 3.12.14 | 사용 중 |
| 원본 저장(Bronze) | SeaweedFS(S3 호환) + boto3 | 4.48 / 1.43.115 | 사용 중 |
| 정제·분석 DB(Silver·Gold) | PostgreSQL + psycopg | 16.4 / 3.3.6 | 사용 중 |
| 스트리밍 | Apache Kafka(KRaft) + confluent-kafka | 4.3.1 / 2.15.1 | 사용 중 |
| 변환·품질 테스트 | dbt-core + dbt-postgres | 1.12.6 / 1.11.0 | 사용 중 |
| 모델링 | numpy(Poisson·Dixon-Coles 직접 구현) | 2.5.4 | 사용 중(3b) |
| 오케스트레이션 | Apache Airflow(standalone, LocalExecutor) | 3.3.2 | 사용 중 |
| 실행 환경 | Docker Compose(프로필 `streaming`, `airflow`) | — | 사용 중 |
| CI | GitHub Actions + pytest | checkout@v5, setup-python@v6 / pytest 9.1.1 | 사용 중 |
| 개인 앱 | Streamlit + SQLite | 1.65.0 / 표준 라이브러리 | 사용 중(기존 카탈로그 앱) |
| 알림 | 텔레그램 Bot API(표준 라이브러리 urllib) | — | 사용 중(4b), 실제 전송은 토큰 설정 후 |
| 배당 API | The Odds API(무료 Starter, 월 500크레딧) | v4 | 사용 중(5b): 다섯 리그, 경기 24·1시간 전, Pinnacle 우선 |
| 문서 그림 | HTML → Edge headless → PNG | — | 사용 중 |
| 레이크 파일 형식 | Parquet(pyarrow) | 25.0.1 | 사용 중(4a) |
| 분산 처리 | Apache Spark(pyspark, 단일 컨테이너 local 모드) | 4.1.3 | 사용 중(4a). 클러스터 실행은 계획 |
| 배포 | Kubernetes(kind·k3d) + Helm | — | **계획** |
| 모바일 보기 | 텔레그램 알림(4b)으로 대체. 조회 웹 화면(FastAPI)은 4c에서 필요하면 | — | 웹 화면은 **계획** |

계획 항목은 아직 코드가 없습니다. 면접에서 "써 봤다"고 말하면 안 되고, "왜 아직 안 썼고 언제 쓸지"로 설명해야 합니다.

## 전체 흐름에서의 위치

```text
openfootball(자동) ─┐
football-data.co.uk ┼─ 배치 수집(Airflow DAG) ─┐
(사람이 받은 CSV)   ┘                          ├→ Bronze(SeaweedFS) → Silver(Postgres) → Gold(dbt) → 모델·화면
football-data.org ── 수집기 → Kafka → 소비자 ──┘
                                  모두 Docker Compose 위에서, 검증은 GitHub Actions
```

## 기술별 정리

### Python 3.12

- **역할:** 수집기, Kafka 생산자·소비자, 스냅샷, 데이터셋 검증, 테스트 전부.
- **왜:** 데이터 엔지니어링 도구(Airflow, dbt, Kafka 클라이언트, boto3)가 모두 Python을 1순위로 지원한다. 앱(Streamlit)도 Python이라 언어를 하나로 유지했다.
- **버전 고정:** 컨테이너는 `python:3.12.14-slim-bookworm`, 로컬 가상환경도 3.12.14. Airflow 이미지가 Python 3.12 기반이라 맞췄다.

### PostgreSQL 16.4 (Silver·Gold)

- **역할:** 원본 목록(`ingest`), 정제·버전 데이터(`silver`), 경기 전 스냅샷(`snapshots`), dbt 결과(`staging`, `gold`).
- **왜:**
  - 데이터가 수십만 행(배당 166,589행) 규모라 웨어하우스나 레이크 없이 충분하다.
  - "덮어쓰지 않는다"를 트리거로, "점수는 0 이상" 같은 형식을 CHECK 제약으로 DB가 직접 강제할 수 있다. 코드 실수가 있어도 잘못된 데이터가 들어가지 않는다.
  - 시점 조회(`silver.*_known_at(as_of)`)를 SQL 함수로 한곳에 둘 수 있다.
  - 수집 기록과 데이터 반영을 한 트랜잭션으로 묶을 수 있다.
- **대안과 판단:**
  - BigQuery·Snowflake: 비용과 외부 의존이 생기고, 이 규모에서는 이점이 없다.
  - Delta Lake·Iceberg: 시간 여행 기능이 있지만, 이 프로젝트는 "발표 시각·수신 시각" 두 축이 필요해 직접 설계했다. 데이터가 커지면 Gold 일부를 옮길 계획.
- **쓰는 곳:** `pipelines/sql/001~004_*.sql`, `pipelines/db.py`(마이그레이션, advisory lock).
- **로컬 포트:** 127.0.0.1:55432(외부에 열지 않음).

### psycopg 3.3.6

- **역할:** Python에서 Postgres 접근.
- **왜:** `COPY`를 지원해 배당 16만 행을 임시 테이블로 빠르게 넣고, SQL 한 번으로 "바뀐 것만 추가"할 수 있다(`football_data_co_uk.write_odds`). 연결에 `connect_timeout=10`을 넣어 일시적인 연결 지연에 무한히 기다리지 않게 했다.
- **대안:** psycopg2는 새 기능 개발이 psycopg 3로 넘어갔고, SQLAlchemy ORM은 이 프로젝트의 SQL 중심 설계에 비해 과하다.

### SeaweedFS 4.48 + boto3 1.43.115 (Bronze)

- **역할:** 받은 원본 파일을 바이트 그대로 보존하는 S3 호환 객체 저장소.
- **왜 S3 호환인가:** 실무의 원본 보관은 대부분 S3다. 코드가 boto3(S3 표준 SDK)만 쓰므로, 나중에 AWS S3로 바꿀 때 접속 주소와 키만 바꾸면 된다.
- **왜 SeaweedFS인가:** 처음에는 MinIO를 쓰려 했지만 이미지를 받을 수 없었다(Docker Hub 거부, quay.io 401). 그래서 S3 API를 제공하고 이미지 버전을 고정할 수 있는 SeaweedFS로 바꿨다. 코드는 boto3라 바뀐 것이 없다.
- **설정에서 배운 점:** 테스트마다 버킷을 만들자 "No free volumes"가 났다. 볼륨 수를 디스크에 맞춰 자동 계산(`-volume.max=0`)하고 볼륨 하나를 256MB로 줄였으며, 테스트는 버킷 하나를 재사용하게 했다([예외 처리 기록](error_handling.md)).
- **쓰는 곳:** `pipelines/object_store.py`(경로 방식 주소, `put_if_absent`로 같은 내용은 한 번만 저장).

### Apache Kafka 4.3.1 (KRaft) + confluent-kafka 2.15.1

- **겪은 문제:** 새 소비자 그룹은 파티션 배정까지 몇 초 걸린다. 테스트용 "메시지가 없으면 종료" 모드가 배정 전 빈 응답을 세다가 CI에서 메시지를 하나도 읽지 않고 끝났다. 배정 후부터 세도록 고쳤다([예외 처리 기록](error_handling.md) 16번).

- **왜 Kafka인가:**
  - 수집과 저장을 분리한다. DB가 잠시 멈춰도 이벤트는 Kafka에 남아 있다가 다시 처리된다.
  - 오프셋을 처음으로 돌려 전체를 다시 처리(재생)할 수 있다. 실제로 380개 이벤트를 다시 처리해 Silver가 그대로인 것을 확인했다.
  - 데이터 엔지니어 채용에서 가장 많이 요구하는 스트리밍 기술이다.
- **왜 KRaft인가:** Kafka 4부터 ZooKeeper가 없어졌다. 브로커 하나로 컨트롤러까지 맡아 로컬 구성이 단순하다.
- **설정 선택:**
  - 토픽 자동 생성 끔(`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`): 오타 난 토픽이 조용히 생기지 않게 코드(`kafka_io.ensure_topics`)로만 만든다.
  - 생산자 `acks=all` + 멱등(idempotence): 재전송 때 중복 기록을 막는다.
  - 소비자는 DB 커밋 **뒤에** 오프셋을 직접 커밋: 최소 한 번 처리(at-least-once). 중복은 event_id(내용 해시)로 걸러 결과적으로 한 번만 반영된다.
  - 힙 512MB로 제한: 노트북에서 다른 서비스와 함께 돌리기 위해.
- **왜 Kafka 트랜잭션(exactly-once)을 안 썼나:** Kafka의 exactly-once는 Kafka 안(읽기→쓰기)에서만 보장된다. 이 프로젝트의 최종 저장소는 Postgres라, 어차피 Postgres 쪽에서 중복을 걸러야 한다. 그래서 "최소 한 번 + 멱등 저장"이 더 단순하고 정확하다.
- **대안:** Redpanda(Kafka 호환, 가볍다)도 가능했지만, 면접에서 설명할 표준 구성과 공식 이미지를 택했다. 클라이언트는 kafka-python 대신 librdkafka 기반으로 멱등 생산자를 제대로 지원하는 confluent-kafka를 썼다.

### dbt-core 1.12.6 + dbt-postgres 1.11.0 (Gold)

- **역할:** Silver를 읽어 순위표, 1X2 확률, 시간순 학습 데이터셋을 SQL로 만들고 품질 테스트를 돌린다.
- **왜:** 변환 규칙을 SQL 파일로 버전 관리하고, 모델 간 의존(계보)을 dbt가 계산하며, 테스트가 실패하면 빌드가 실패한다. 현재 모델 8개, 테스트 26개(3g에서 경기 기록 추가).
- **설계:** 시점 선택은 Silver 함수에 맡기고 dbt는 `as_of` 변수만 넘긴다(규칙을 두 곳에 두지 않음).
- **겪은 문제:**
  - Streamlit과 protobuf 버전이 충돌해 별도 가상환경(`.venv-dbt`, Airflow 이미지 안에서는 `/home/airflow/dbt-venv`)에 설치했다.
  - Windows 기본 인코딩(CP949) 때문에 한글 SQL 주석을 못 읽어 `PYTHONUTF8=1`을 넣었다(`pipelines/dbt_runner.py`).
- **대안:** Python(pandas)으로 집계할 수도 있지만, SQL로 두면 DB 안에서 처리되고 다른 사람이 읽기 쉽다. 3a의 누수 검사는 일부러 Python으로 한 번 더 구현해 dbt 결과와 비교한다.

### numpy 2.5.4 (기준 모델)

- **역할:** 3b 기준 모델(Poisson, Dixon-Coles)의 학습과 점수 확률표 계산.
- **왜 직접 구현했나:** Poisson 로그 선형 모델은 IRLS(반복 가중 최소제곱)로 몇 줄이면 학습된다. 직접 구현해서 ridge 벌점(기록이 적은 팀을 리그 평균 쪽으로 축소)과 시간 감쇠(반감기)를 원하는 방식으로 넣었고, 계수 하나하나를 설명할 수 있다. 한 번 학습에 수 밀리초, 백테스트 전체(실행 36개, 주 단위 재학습 1,473번)가 약 20초.
- **대안과 판단:** scikit-learn의 `PoissonRegressor`는 관측치 가중치와 벌점을 지원하지만 Dixon-Coles 보정은 없다. statsmodels·scipy는 설치하지 않았다(의존성을 늘릴 만큼 필요하지 않음). Dixon-Coles를 원 논문처럼 모든 계수를 함께 최대화하려면 범용 최적화기(scipy)가 필요해, 지금은 두 단계 근사를 쓰고 그렇다고 문서에 남겼다.
- **검증:** 정해 둔 계수로 가상 리그를 만들어 학습하면 그 계수를 되찾는지 테스트한다(`tests/test_models.py`).
- **버전:** 원래 Streamlit을 통해 간접 설치됐는데, 직접 쓰게 되면서 `requirements.txt`에 고정했다.
- **쓰는 곳:** `pipelines/modeling/models.py`, `pipelines/modeling/backtest.py`, `pipelines/modeling/evaluation.py`(3c 블록 부트스트랩: 주 117개를 2,000번 복원 추출해도 numpy 배열 연산으로 1초 안).
- **통계 라이브러리 없이:** Wilson 구간·ECE·부트스트랩은 식이 짧아 직접 구현하고 알려진 값으로 테스트했다.

### Apache Airflow 3.3.2

- **역할:** 매일 배치(마이그레이션 → 시즌 결정 → 시즌별 수집 → co.uk 파일 수집 → dbt build), 30분마다 경기 전 스냅샷과 모델 예측, 화요일 06:00 UTC 주간 채점 보고서(DAG 3개). 매일 DAG 끝에서 Spark 재계산·대조(4a).
- **왜:** 데이터 엔지니어링의 표준 오케스트레이터다. 재시도, 실행 기록, 동적 작업(`ingest_season.expand`로 시즌 수만큼 작업 생성)을 제공한다.
- **구성:** standalone + LocalExecutor. 개인 PC 한 대라 Celery·Kubernetes 실행기는 과하다. Airflow 메타데이터는 같은 Postgres 서버 안의 별도 데이터베이스(`airflow`)에 두어 프로젝트 데이터(`sports`)와 섞이지 않게 했다(`airflow-db-init`이 한 번만 만든다).
- **경계:** 상시 실행(수집기·소비자)은 Airflow에 넣지 않고 별도 컨테이너로 돌린다. Airflow는 "정해진 시각에 끝나는 작업"용이다.
- **대안:** Dagster(데이터 자산 중심)도 좋지만, 채용 공고에서 훨씬 많이 쓰이는 Airflow를 택했다.
- **쓰는 곳:** `dags/openfootball_epl_daily.py`, `dags/prematch_snapshots.py`, `dags/weekly_model_report.py`, `infra/airflow/Dockerfile`(3d에서 numpy 추가).
- **배포 순서에서 배운 점:** 코드는 볼륨으로 바로 반영되지만 라이브러리는 이미지에 있다. 스냅샷 DAG가 numpy를 쓰기 전에 이미지를 먼저 다시 빌드하고, 컨테이너 안에서 import를 확인한 뒤 코드를 연결했다. 로컬 주소 127.0.0.1:58080.

### Docker Compose

- **역할:** Postgres, SeaweedFS, Kafka(기본), 수집기·소비자(`streaming` 프로필), Airflow(`airflow` 프로필)를 한 파일로 실행.
- **왜 프로필:** 테스트할 때는 저장소만, 운영할 때는 전부. 필요한 만큼만 띄워 노트북 자원을 아낀다.
- **원칙:** 모든 포트는 127.0.0.1에만 연다. 코드는 이미지에 굽지 않고 읽기 전용 볼륨으로 연결해 수정이 바로 반영된다. 이미지 태그는 `latest`를 쓰지 않고 정확한 버전으로 고정한다.

### GitHub Actions + pytest 9.1.1

- **역할:** 푸시할 때 실제 Postgres·SeaweedFS·Kafka를 띄우고 전체 테스트와 종단 간 실행을 돌린다.
- **구성:** Postgres는 서비스 컨테이너, SeaweedFS와 Kafka는 `docker run`으로 띄운다(서비스 컨테이너는 실행 명령을 바꿀 수 없는데, 두 서비스는 실행 명령·환경 설정이 필요하다).
- **`REQUIRE_INTEGRATION=1`:** 로컬에서는 서비스가 없으면 통합 테스트를 건너뛰지만, CI에서는 건너뛰기를 실패로 본다. "건너뛰어서 통과"를 막기 위해서다.
- **최근 결과:** 마지막으로 GitHub에서 돈 CI는 run 39550436142(테스트 289개, dbt `PASS=28`). 이후 로컬에서는 dbt `PASS=34`(모델 8 + 테스트 26), 전체 테스트 수는 해설 문서에 단계별로 남긴다.

### Streamlit 1.65.0 + SQLite (기존 개인 앱)

- **역할:** 프로젝트를 시작할 때부터 있던 종목 카탈로그·근거 화면(`app.py`, `src/`). 127.0.0.1에만 연다.
- **왜:** Python만으로 화면을 빨리 만들 수 있다. 데이터 엔지니어링 파이프라인과는 별개로 유지 중이며, 모델 결과를 보여 주는 화면은 3d 이후 계획이다.

### Apache Spark 4.1.3 + Parquet(pyarrow 25.0.1) (4a)

- **역할:** Gold를 Parquet로 S3 `lake` 버킷에 내보내고, 시간순 특징을 Spark로 다시 계산해 dbt 결과와 대조한다.
- **왜 지금:** 자료가 커서가 아니다(Postgres가 몇 초에 처리). 자료가 커져 Spark로 옮길 때 결과가 바뀌지 않는다는 것을 미리 증명하려는 것이다. 2,660경기 × 18열 대조 불일치 0.
- **구성:** Java 17 + pyspark를 담은 컨테이너(`infra/spark/Dockerfile`, compose `spark` 프로필). 매일 자동 실행은 Airflow 이미지 안의 별도 가상환경(`/home/airflow/spark-venv`)에서 한다. Airflow에 docker.sock(사실상 관리자 권한)을 넘기지 않기 위해서다. Windows에서 Spark로 파일을 쓰려면 winutils가 필요해 Linux 컨테이너로 돌린다. 미니 PC·쿠버네티스로 그대로 옮길 수 있다.
- **S3 연결:** s3a 커넥터 대신 boto3로 내려받고 올린다. s3a는 hadoop-aws와 수백 MB의 AWS SDK가 필요해 수 MB 자료에는 과하다. 커지면 바꾼다.
- **Parquet:** 시즌별 Hive 방식 파티션, 열 형식 명시, 시각은 UTC 마이크로초(Spark 기본 INT96은 다른 도구가 시간대 없이 읽음).
- **대안:** DuckDB(한 대에서 Parquet를 SQL로 빠르게)도 맞지만, 여러 대로 늘어나는 경로와 채용 수요를 생각해 Spark를 골랐다.
- **쓰는 곳:** `pipelines/lake/`, `tests/test_lake.py`, `tests/test_spark_features.py`(CI는 Spark 이미지 안에서 `REQUIRE_SPARK=1`로 실행).

### 문서 그림(Edge headless)

- **역할:** `docs/diagrams/*.html`을 PNG로 렌더링(`scripts/render_diagrams.ps1`).
- **왜:** 그림을 HTML로 두면 글자를 고치고 다시 렌더링하기 쉽고, 차이가 Git에 남는다.

## 계획 중인 기술(아직 없음)

| 기술 | 쓸 곳 | 아직 안 쓴 이유 | 도입 조건 |
|---|---|---|---|
| Spark 클러스터·s3a | 여러 대에서 Spark 실행, S3 직접 읽기 | 지금은 단일 컨테이너로 충분(4a) | 리그 확장으로 자료가 커질 때, 쿠버네티스 단계에서 Spark on Kubernetes와 함께 |
| Kubernetes + Helm | 수집기·소비자·Postgres 배포, 재시작·자원 제한 | Docker Compose로 개인 PC 운영은 충분하다 | 모델·API까지 서비스가 늘었을 때 kind·k3d로 로컬 클러스터부터 |
| FastAPI + 모바일 웹 | 휴대폰에서 화면으로 조회 | 지금 볼 것(경기 전 기록, 주간 채점, 장애)은 텔레그램 알림(4b)으로 충분하다. 화면을 만들면 집 PC로 들어오는 길(Tailscale 등)이 필요하다 | 화면이 필요해질 때, 미니 PC와 함께 |

## 버전을 고정하는 이유

- 모든 Python 패키지는 `==`로, 모든 이미지는 정확한 태그로 고정한다(`requirements*.txt`, `docker-compose.yml`).
- 같은 코드가 6개월 뒤에도 같은 결과를 내야 한다. 예측 스냅샷과 모델 버전을 남기는 프로젝트라, 실행 환경도 재현돼야 의미가 있다.
- 실제로 dbt가 의존하는 protobuf가 Streamlit과 충돌한 것처럼, 버전이 풀려 있으면 언제 깨질지 모른다.

## 면접에서 나올 만한 질문

**Q. 데이터가 작은데 Kafka가 꼭 필요했나요?**
양으로만 보면 필요 없습니다. Kafka를 쓴 이유는 처리 방식입니다. 경기 갱신은 언제 올지 모르는 이벤트이고, DB가 잠시 멈춰도 잃으면 안 되며, 규칙을 고친 뒤 처음부터 다시 처리할 수 있어야 합니다. 실제로 오프셋을 처음으로 돌려 380개를 다시 처리했고, 중복으로 걸러져 Silver가 바뀌지 않는 것을 확인했습니다.

**Q. 데이터가 작은데 Spark는 왜 썼나요?**
크기 때문은 아닙니다. 지금 데이터는 Postgres가 몇 초에 처리합니다. 자료가 커져 Spark로 옮길 때 결과가 바뀌지 않는다는 것을 미리 증명하려고, 같은 특징을 dbt와 Spark로 각각 구현해 2,660경기 × 18개 열을 대조했고 불일치는 0이었습니다. 운영 경로는 여전히 dbt이고, Spark는 옮겨 갈 길을 검증해 둔 것입니다.

**Q. Bronze를 왜 DB가 아니라 객체 저장소에 두었나요?**
원본 파일은 크기가 제각각이고 파싱하지 않은 바이트 그대로 둬야 합니다. 객체 저장소가 그 용도에 맞고, 내용 해시를 경로로 쓰면 같은 파일은 자동으로 한 번만 저장됩니다. S3 API를 쓰므로 클라우드로 옮기기도 쉽습니다.

**Q. MinIO가 아니라 SeaweedFS를 쓴 이유는요?**
처음엔 MinIO를 쓰려 했는데 이미지를 받을 수 없었습니다. 코드가 S3 표준 SDK(boto3)만 쓰도록 만들어 두었기 때문에 저장소만 SeaweedFS로 바꾸고 코드는 그대로였습니다. 특정 제품이 아니라 S3 API에 맞춰 설계한 덕분입니다.

**Q. 모델은 왜 라이브러리를 안 쓰고 직접 만들었나요?**
축구 득점의 표준 기준선인 Poisson은 IRLS로 몇 줄이면 학습됩니다. 직접 구현해서 기록이 적은 팀을 리그 평균으로 당기는 벌점과 오래된 경기를 덜 보는 시간 감쇠를 원하는 방식으로 넣었고, 가상 데이터로 정답 계수를 되찾는 테스트로 구현을 검증했습니다. 복잡한 모델은 이 기준선을 넘는지 보는 3c 이후에 검토합니다.

**Q. Airflow와 Kafka 소비자는 어떻게 나눴나요?**
Airflow는 "정해진 시각에 시작해서 끝나는 작업"(일일 배치, 30분 스냅샷)을 맡고, Kafka 소비자와 API 수집기는 계속 떠 있는 별도 컨테이너입니다. 상시 프로세스를 Airflow 작업으로 돌리면 작업 슬롯을 계속 차지하고 재시작 관리도 어색해집니다.

**Q. dbt를 왜 별도 가상환경에 설치했나요?**
dbt가 요구하는 protobuf 버전이 Streamlit과 충돌했습니다. 한 환경에 억지로 맞추면 둘 중 하나가 깨지므로, dbt는 별도 환경에 두고 `pipelines/dbt_runner.py`가 그 실행 파일을 찾아 호출합니다. Airflow 이미지 안에서도 같은 방식입니다.

**Q. CI에서 통합 테스트를 어떻게 믿을 수 있나요?**
CI에서 실제 Postgres, SeaweedFS, Kafka를 띄우고, `REQUIRE_INTEGRATION=1`로 통합 테스트가 건너뛰어지면 실패하게 했습니다. 서비스가 안 떠서 테스트가 몰래 건너뛰어지고 초록불이 되는 경우를 막기 위해서입니다.
