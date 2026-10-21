# 4a. 데이터 레이크와 Spark: Gold → Parquet → Spark 재계산 → dbt와 대조

작성: 2026-10-21.

## 한 줄 요약

Gold 테이블을 Parquet 파일로 S3 lake 버킷에 내보내고, 같은 시간순 특징(3a의 `gold.match_features_pit`)을 Spark로 다시 계산했습니다. dbt(SQL) 결과와 행 단위로 대조하니 **2,660경기 × 18개 열에서 불일치 0**이었습니다. 같은 규칙을 다른 엔진과 다른 방식으로 구현해도 결과가 같다는 것을 보인 것이, 이 단계에서 검증한 내용입니다.

## 왜 지금 Spark인가

지금 자료(경기 2,660, 팀 기록 4,700행)는 Postgres가 몇 초에 처리해서, 크기 때문에 Spark가 필요한 것은 아닙니다. 그래도 지금 하는 이유는 세 가지입니다.

1. **옮겨 갈 길을 미리 검증합니다.** 여러 리그·수십 시즌·이벤트 단위 자료로 커지면 SQL 하나로는 버겁습니다. 그때 갑자기 Spark로 다시 짜면 결과가 같은지 보장할 수 없습니다. 지금 작은 자료로 "Spark 구현 = dbt 구현"을 증명해 두면 옮기는 순간에도 믿을 수 있습니다.
2. **레이크 형식(Parquet)을 도입합니다.** 열 단위 압축 파일이라 분석 도구(Spark, pandas, DuckDB 등)가 바로 읽습니다.
3. **포트폴리오에서 비어 있던 기술입니다.** 다만 "작은 자료에 Spark를 썼다"가 아니라 "엔진을 바꿔도 결과가 같은지 대조했다"로 설명해야 합니다.

## 흐름

```text
python -m pipelines.lake.run all   (Spark 컨테이너, docker compose --profile spark)

1. export     Postgres Gold ──pyarrow──→ s3://lake/exports/<스냅샷>/
                team_match_results/season=2025-26/part-0.parquet   ← Spark 입력
                matches/season=…/part-0.parquet                     ← 특징을 계산할 경기
                dbt_match_features/season=…/part-0.parquet          ← 대조의 정답(dbt 결과)
                _manifest.json (파일·행 수·SHA-256), exports/latest.json(가장 최근 스냅샷)
2. spark      내려받기 → Spark local[2]: 조인 + row_number 창 함수 → s3://lake/spark/match_features_pit/snapshot=<스냅샷>/
3. reconcile  pyarrow로 두 Parquet를 읽어 경기 × 열 단위 비교 → 불일치 0이어야 종료 코드 0
```

## 설계 결정과 이유

| 결정 | 위치 | 이유 |
|---|---|---|
| lake 버킷을 따로 | `pipelines/config.py`의 `lake_bucket`, `.env.example`의 `LAKE_BUCKET` | Bronze는 "받은 원본 그대로"만 두는 곳이다. 가공한 Parquet를 섞으면 층의 규칙이 흐려진다 |
| 스냅샷 ID = 내용 해시 | `pipelines/lake/export.py`의 `snapshot_id` | 같은 Gold를 다시 내보내면 같은 경로에 같은 파일이라 다시 올리지 않는다(멱등). Bronze의 내용 해시 경로와 같은 생각 |
| 정답(dbt 결과)을 같은 스냅샷에 | `TABLES["dbt_match_features"]` | 내보낸 뒤 Gold가 다시 만들어져도(매일 DAG) "같은 입력으로 dbt가 낸 값"과 비교할 수 있다 |
| Hive 방식 시즌 파티션 | `…/season=2025-26/…` | Spark가 경로에서 시즌 열을 읽고, 시즌 단위로 필요한 파일만 읽을 수 있다 |
| Spark는 조인 + 창 함수 | `pipelines/lake/spark_features.py`의 `compute_features` | dbt는 경기마다 LATERAL 하위 쿼리로 최근 N경기를 고른다. 같은 조건(시작 + 3시간 ≤ 대상 시작, 대상 경기 제외)을 다른 방식으로 구현해야 대조에 의미가 있다 |
| 시각은 표준 Parquet 형식 | `spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS` | Spark 기본값(INT96)은 다른 도구가 시간대 없이 읽어 대조에서 어긋난다 |
| 시즌 값을 추측하지 않음 | `partitionColumnTypeInference.enabled=false` | "2025-26"이 다른 형식으로 해석되지 않게 |
| S3는 boto3로 받고 올림 | `download_prefix`, `put_if_absent` | Spark의 S3 커넥터(s3a)는 hadoop-aws와 수백 MB의 AWS SDK 묶음이 필요하다. 수 MB 자료에는 과하다. 자료가 커지면 s3a로 바꾼다(문서에 남김) |
| 대조는 Spark 밖에서 | `pipelines/lake/reconcile.py`(pyarrow) | 검사하는 쪽이 검사받는 엔진에 기대지 않는다 |
| 허용 오차 | `TOLERANCE` | dbt가 평균을 넷째 자리, 쉰 날 수를 둘째 자리에서 반올림해 저장하므로 그만큼(0.00006, 0.006)만 허용 |
| Spark는 컨테이너에서만 | `infra/spark/Dockerfile`, compose `spark` 프로필 | Windows에서 Spark로 파일을 쓰려면 별도 도구(winutils)가 필요해 흔히 깨진다. 컨테이너(Linux)라 미니 PC·쿠버네티스로 그대로 옮긴다 |
| 수집 API 키를 넘기지 않음 | compose `spark-features` 환경 변수 | 필요한 접속 정보만(최소 권한) |
| 결과가 있으면 다시 계산 안 함 | `spark_features.run` | 같은 스냅샷의 결과 매니페스트가 있으면 건너뛴다(다시 실행 4초) |
| CI에서 Spark 테스트 | `.github/workflows/ci.yml`, `REQUIRE_SPARK=1` | 호스트 가상환경에는 pyspark가 없어 Spark 테스트가 건너뛰어진다. CI는 Spark 이미지를 빌드해 그 안에서 돌리고, 건너뛰면 실패로 본다 |

## 2026-10-21 확인 결과

| 확인 | 결과 |
|---|---|
| 내보내기 | 스냅샷 `3856a9097e28b3ff`, 파일 21개(3개 테이블 × 7시즌), team_match_results 4,700행, matches 2,660행, dbt 특징 2,660행 |
| 다시 내보내기 | 같은 스냅샷, 새로 올린 파일 0 |
| Spark | Spark 4.1.3, 2,660행 계산 |
| 대조 | 2,660경기 × 18개 열, **불일치 0**, 빠진 행 0, 남는 행 0, 통과 |
| 다시 실행 | 계산을 건너뜀(4초) |
| 호스트에서 대조 | 같은 결과(Spark 없이 pyarrow만으로) |
| Spark 이미지 | `sports-prediction-spark:4.1.3`, 1.79GB(Java 17 + pyspark) |
| 테스트 | 호스트 4개 + Spark 컨테이너 2개 추가. 전체(호스트) 309개 통과, 1개는 Spark 전용이라 건너뜀(CI는 컨테이너에서 실행) |

## 직접 확인하는 방법

전체(내보내기 → Spark → 대조):

```bash
docker compose --env-file .env -f infra/docker-compose.yml --profile spark run --rm spark-features
```

처음 실행하면 Spark 이미지를 빌드합니다(몇 분). 마지막의 `"passed": true`, `"mismatches": 0`을 확인합니다.

Spark 없이 호스트에서 내보내기·대조만:

```bash
.\.venv\Scripts\python.exe -m pipelines.lake.run export
```

```bash
.\.venv\Scripts\python.exe -m pipelines.lake.run reconcile
```

Spark 테스트(컨테이너 안):

```bash
docker compose --env-file .env -f infra/docker-compose.yml --profile spark run --rm -e REQUIRE_SPARK=1 spark-features python -m pytest tests/test_spark_features.py -q
```

## 테스트가 확인하는 것

| 테스트 | 확인 내용 |
|---|---|
| `test_snapshot_id_depends_only_on_content` | 내용이 같으면 같은 스냅샷, 시각 하나만 달라도 다른 스냅샷 |
| `test_parquet_keeps_types_and_utc_times` | 열 형식과 UTC 시각이 그대로 왕복, 빈 표도 쓸 수 있음 |
| `test_reconcile_tolerances` | 반올림만큼은 같다고 보고, 그보다 큰 차이·경기 수 차이·NULL 차이는 불일치 |
| `test_export_is_idempotent_and_reconcile_detects_differences` | 다시 내보내면 새 파일 0, 정답을 그대로 두면 통과, 값 하나만 바꾸면 그 칸 하나만 불일치로 잡음 |
| `test_three_hour_rule_target_exclusion_and_last_ten`(Spark) | Spark 결과가 3a의 Python 독립 재계산과 같음. 2시간 59분 전 경기 제외·딱 3시간 전 포함, 대상 경기 제외, 최근 10경기만, 기록 없는 팀은 0경기·NULL |
| `test_rest_days_and_last_feature_kickoff`(Spark) | 쉰 날 수, 특징에 쓴 가장 최근 경기 시각 |

## 면접에서 나올 만한 질문

**Q. 데이터가 작은데 Spark를 왜 썼나요?**
크기 때문은 아닙니다. 지금은 Postgres가 몇 초에 처리합니다. 자료가 커져 Spark로 옮길 때 결과가 바뀌지 않는다는 것을 미리 증명하려고 했습니다. 같은 특징을 dbt(SQL)와 Spark로 각각 구현하고 2,660경기 × 18개 열을 대조해 불일치 0을 확인했습니다.

**Q. 두 구현이 같다는 걸 어떻게 확인했나요?**
세 겹입니다. dbt 결과와 Spark 결과를 같은 스냅샷에서 행·열 단위로 대조하고, 대조는 Spark가 아닌 pyarrow로 합니다. Spark 단위 테스트는 3a에서 만든 Python 독립 재계산을 정답으로 씁니다. 그리고 대조 코드 자체도 값 하나를 바꾸면 그 칸 하나만 잡는지 테스트했습니다.

**Q. 왜 Spark가 S3를 직접 읽지 않나요?**
s3a 커넥터는 hadoop-aws와 수백 MB의 AWS SDK가 필요합니다. 지금 자료는 수 MB라 boto3로 내려받아 local 모드로 읽는 게 단순합니다. 자료가 커지면 s3a로 바꾸고, 그때도 같은 대조로 결과가 같은지 확인하면 됩니다.

**Q. 내보낸 뒤 Gold가 바뀌면 대조가 깨지지 않나요?**
정답(dbt 결과)도 같은 스냅샷 안에 Parquet로 함께 내보냈습니다. 그래서 Spark 결과는 항상 같은 입력으로 dbt가 낸 값과 비교됩니다. 스냅샷 ID는 내용 해시라, Gold가 바뀌면 새 스냅샷이 생기고 이전 것도 남습니다.

**Q. Parquet와 파티션은 어떻게 나눴나요?**
시즌별 Hive 방식 경로(`season=2025-26`)입니다. 분석은 대부분 시즌 단위라 필요한 시즌 파일만 읽을 수 있습니다. 열 형식은 명시해서 시즌마다 형식이 달라지지 않게 했고, 시각은 UTC 마이크로초 표준 형식으로 썼습니다.

## 한계

- **Spark는 단일 컨테이너 local 모드입니다.** 클러스터(여러 대) 실행은 하지 않았습니다. 쿠버네티스 단계(4c 예정)에서 Spark on Kubernetes를 검토합니다.
- **s3a를 쓰지 않습니다.** 자료가 커지면 바꿔야 합니다.
- **Iceberg·Delta 같은 테이블 형식은 아직 없습니다.** 스냅샷은 경로와 매니페스트로 관리합니다.
- **Airflow DAG에 아직 넣지 않았습니다.** 지금은 명령으로 실행합니다. Gold가 매일 다시 만들어지므로, 매일 DAG 끝에 붙이는 것이 다음 후보입니다.
- **Spark 이미지가 큽니다(1.79GB).** Java와 Spark 전체가 들어 있기 때문입니다.
