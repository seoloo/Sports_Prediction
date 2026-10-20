# 해외축구 데이터 플랫폼 로드맵 v1


## 목표

1. 데이터 엔지니어링 포트폴리오: 배치·스트리밍 수집, 데이터 레이크, 품질 검증, 오케스트레이션, 컨테이너 배포를 실제 도메인 문제로 보여줍니다.
3. 모바일 확인: 휴대폰에서 예측과 기록을 조회합니다. 외부 접속 방식은 4단계에서 별도로 결정합니다.

## 바꾸지 않는 원칙

- published_at, received_at, as_of에 따른 시점 선택과 누수 방지
- 덮어쓰지 않는 버전 저장, 원본 해시와 수집 이력 보존
- 사용권 검토 전 소스 비활성, 목적별 사용권 구분
- 자료·규칙·모델 검증이 부족하면 WITHHELD와 이유 표시
- 서비스는 기본적으로 127.0.0.1에만 바인딩

기존 `src/`의 검증·규칙·시점 로직은 도메인 핵심으로 재사용합니다. 기존 Streamlit 앱과 SQLite는 4단계 서빙 전환 전까지 그대로 동작합니다.

## 목표 구조

```text
openfootball(CC0) ──자동 수집──┐
football-data.co.uk CSV ─수동 다운로드→ landing 폴더 ─┤
football-data.org API ──폴링(2단계)── Kafka ─┤
                                         ↓
                Bronze: MinIO 원본(파일 그대로 + 해시·received_at)
                                         ↓
                Silver: 정규화 경기·결과 버전, 팀 별칭 매핑(Postgres)
                                         ↓
                Gold: 시점 기준 특징·백테스트 데이터셋(dbt / Spark)
                                         ↓
                모델 학습·평가 → 예측 스냅샷 → API·화면
Airflow가 전체 작업 순서와 재시도를 관리합니다.
```

## 기술 선택과 이유

| 역할 | 선택 | 이유 |
|---|---|---|
| 운영·분석 DB | PostgreSQL 16 | 버전 테이블, 트리거, 제약을 그대로 옮길 수 있고 dbt·Airflow와 호환 |
| 원본 저장 | MinIO + Parquet | S3 호환 객체 저장소로 Bronze 원본 보존. 테이블 형식(Iceberg)은 Spark 단계에서 검토 |
| 스트리밍 | Apache Kafka(KRaft 모드) | 결과 정정·배당 변경·일정 변경을 추가 전용 이벤트로 다룸. 과거 시즌 재생으로 스트리밍 처리 검증 |
| 대용량 처리 | Apache Spark | 여러 리그·시즌 재처리, StatsBomb 이벤트 수준 데이터(연구 전용) 처리 |
| 변환·품질 | dbt + 테스트 | Gold 모델 정의와 품질 검사를 코드로 관리 |
| 오케스트레이션 | Apache Airflow | 수집→정제→특징→학습→평가 순서, 재시도, 백필 |
| 실행 환경 | Docker Compose → kind/k3d 쿠버네티스 + Helm | 로컬 개발은 Compose, 마지막에 배포 구성 시연 |
| CI | GitHub Actions | 단위 테스트, 린트, 소규모 통합 테스트 |

데이터 양이 작은 단계에서는 Spark를 쓰지 않습니다. 각 도구는 필요가 생긴 단계에서 추가합니다.

## 단계

| 단계 | 결과물 | 완료 기준 | 사용자 선행 조건 |
|---|---|---|---|
| 0. 소스 검토 | 소스 검토 문서, 이 로드맵 | 소스별 역할·제약 기록 | 없음 |
| 1a. 로컬 인프라 | docker-compose(Postgres, MinIO), 127.0.0.1 바인딩, `.env.example` | `docker compose up` 후 상태 확인 통과 | 없음 |
| 1b. openfootball 배치 수집 | Bronze 원본 저장, Silver 경기·결과 버전, 형식 차이 처리, 팀 별칭 | 중복 수집이 멱등, 결측 전반 점수 보존, 재수집 시 변경만 새 버전 | 없음 |
| 1c. 배당 수집 | landing 폴더의 football-data.co.uk CSV를 Bronze·Silver로 | 수동 다운로드 파일만 처리, 원본 Git 제외 | CSV 수동 다운로드 |
| 1d. 품질·오케스트레이션 | dbt 모델과 테스트, Airflow DAG, CI | DAG 재실행 시 결과 동일, 테스트 통과 | 없음 |
| 2. 스트리밍 | Kafka, football-data.org 폴링 수집기, 과거 시즌 재생기, 수동 배당 이벤트 | 이벤트 중복·지연 도착 처리, 분당 요청 제한 준수 | football-data.org 키 |
| 3. 모델·검증 | EPL Poisson/Dixon-Coles 기준 모델, 시간순 백테스트, 시장 배당 비교, 모의 기록 | 누수 검사 통과, Log loss·Brier를 기준 모델·배당과 비교, 보류율 공개 | 없음 |
| 4. 서빙·배포 | FastAPI 조회 API, 모바일 화면, kind/k3d 배포 | 휴대폰에서 예측 스냅샷 조회 | 외부 접속 방식 결정 |


## 디렉터리 계획

```text
infra/                 # docker-compose, 이후 Helm 차트
pipelines/
  ingest/              # openfootball, football-data.co.uk, football-data.org
  streaming/           # Kafka 생산자·소비자(2단계)
  spark/               # Spark 작업(2~3단계)
dbt/                   # Gold 모델과 테스트
dags/                  # Airflow DAG
src/                   # 기존 도메인 로직(검증·규칙·시점)
```

## 결정 대기

- GitHub 공개 시점: 현재 정책은 main 미푸시 커밋 20개마다 푸시합니다.
- 모바일 외부 접속 방식(4단계): 개인 VPN 등. 127.0.0.1 원칙을 바꾸는 결정입니다.
