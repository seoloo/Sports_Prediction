# 7b. Spark가 Iceberg 테이블을 직접 읽고 쓰기

작성: 2026-10-24. 상태: **완료.**

## 한 줄 요약

7a까지 Spark는 내보내기 Parquet 파일을 boto3로 내려받아 읽고, 결과를 Parquet 파일로 올렸습니다. 이번 단계부터 Spark는 **PyIceberg와 같은 카탈로그(Postgres)로 `gold.*` Iceberg 테이블을 "그 내보내기를 쓴 스냅샷"으로 읽고(시간 여행)**, 결과를 **`spark.match_features_pit`, `spark.nba_standings` Iceberg 테이블**에 내보내기 ID를 단 스냅샷으로 씁니다. dbt ↔ Spark 대조도 Spark 결과를 Iceberg에서 읽습니다.

실제 데이터로 축구 특징 12,264경기 × 18열, NBA 순위표 270행 × 9열 모두 불일치 0입니다. 이제 한 테이블을 두 엔진(Python의 PyIceberg, JVM의 Spark)이 같은 카탈로그로 쓰고 읽습니다.

## 흐름

```text
run all (매일 DAG의 spark_reconcile, Airflow 안 Spark 가상환경)
  export   Gold → Parquet(그대로)
  iceberg  PyIceberg: Parquet → gold.* 스냅샷(내보내기 ID), 모든 스냅샷 검사(7a)
  spark    Spark(Iceberg JDBC 카탈로그 + S3FileIO)
             SELECT * FROM lake.gold.matches VERSION AS OF <그 내보내기의 스냅샷 ID>   (team_match_results, nba_games도)
             → 특징·순위표 계산(4a·6b 규칙 그대로)
             → lake.spark.match_features_pit / nba_standings 에 overwrite, snapshot-property.export.snapshot = 내보내기 ID
             (이미 그 내보내기 결과가 있으면 계산하지 않음)
  reconcile 정답(dbt 내보내기 Parquet) ↔ Spark 결과(PyIceberg로 spark.* 의 그 내보내기 스냅샷)
```

## 바꾼 것

| 변경 | 위치 | 이유 |
|---|---|---|
| Spark 세션에 Iceberg 카탈로그 | `spark_features.iceberg_conf`, `spark_session` | PyIceberg와 같은 카탈로그 이름(`lake`)·Postgres 스키마(`lakehouse`)·창고 경로. 파일 입출력은 Iceberg S3FileIO라 hadoop-aws(s3a)가 필요 없다 |
| 입력: 내보내기 스냅샷으로 시간 여행 | `spark_features.run` | 내려받기(boto3 → 임시 폴더) 대신 테이블. 같은 내보내기를 고정해서 읽으므로 그 뒤에 테이블이 바뀌어도 결과가 같다 |
| 결과: Iceberg 테이블 | `spark_features.write_result` | 시즌 파티션, 전체 덮어쓰기, 스냅샷 요약에 내보내기 ID(Spark 쓰기 옵션 `snapshot-property.*`) |
| 대조 | `reconcile.spark_rows` | Spark 결과를 PyIceberg로 읽음. 그 내보내기의 결과가 없으면 빈 목록 → 대조 실패로 드러남 |
| 공용 함수 | `iceberg.snapshot_for_export`, `write_snapshot` | 쓰기 규칙(첫 쓰기 append, 그 뒤 overwrite)과 내보내기 → 스냅샷 찾기를 한곳에 |
| jar 3개 | `infra/spark/fetch_jars.py`, Spark·Airflow Dockerfile | Maven Central에서 판·SHA-512 고정. 값이 다르면 이미지 빌드 실패 |

| jar | 크기 | 확인 |
|---|---|---|
| `iceberg-spark-runtime-4.1_2.13-1.12.0.jar` | 48.3MB | Maven 공개 SHA-512 |
| `iceberg-aws-bundle-1.12.0.jar`(S3FileIO) | 69.0MB | Maven 공개 SHA-512 |
| `postgresql-42.7.14.jar`(JDBC 카탈로그) | 1.2MB | Maven에 SHA-512가 없어, 공개 SHA-1·MD5와 맞는 것을 확인한 파일에서 계산 |

## 사용한 기술

| 기술 | 어디에 | 어떻게 썼나 |
|---|---|---|
| Iceberg Spark 런타임(`SparkCatalog`, `IcebergSparkSessionExtensions`) | 세션 설정 | Spark SQL에서 `lake.gold.*`를 테이블로 |
| Iceberg JDBC 카탈로그 | `spark.sql.catalog.lake.type=jdbc` | PyIceberg SqlCatalog이 만든 카탈로그 표(`iceberg_tables`)를 Java 쪽이 그대로 읽고 씀 |
| Iceberg S3FileIO(AWS SDK v2 번들) | `io-impl`, `s3.endpoint`, `s3.path-style-access` | SeaweedFS에 경로 방식 주소로. Hadoop 파일시스템을 거치지 않음 |
| SQL 시간 여행(`VERSION AS OF`) | 입력 읽기 | 스냅샷 ID로 그 내보내기의 테이블 |
| DataFrameWriterV2(`writeTo().overwrite()`) + `snapshot-property.*` | 결과 쓰기 | 덮어쓴 스냅샷에 내보내기 ID |
| SHA-512 고정 다운로드 | `fetch_jars.py` | 공급망 확인: 받은 파일이 고정한 값과 다르면 쓰지 않음 |

## 확인 결과(2026-10-24)

| 확인 | 결과 |
|---|---|
| 이미지 빌드 | jar 3개 모두 SHA-512 확인(48,309,426 / 69,034,612 / 1,221,004바이트) |
| 실제 데이터(`run all`, Spark 컨테이너) | 내보내기 `1d01414f29fc689e`의 입력 스냅샷 3개로 계산 → 축구 특징 12,264 × 18열, NBA 순위표 270 × 9열 불일치 0. Iceberg 검사 5개 스냅샷 불일치 0 |
| 다시 실행 | Spark 계산 건너뜀(`newly_computed: false`), Iceberg 쓰기 건너뜀, 대조 통과 |
| 두 엔진이 같은 테이블 | Spark가 쓴 `spark.*`를 PC의 PyIceberg가 그대로 읽음(시즌 파티션, 스냅샷 요약의 내보내기 ID 확인) |
| 테스트 | 대조 테스트 2개를 Iceberg 결과 자리로 바꿈(결과 없음 → 실패, 같은 값 → 통과, 값 하나 바꾼 스냅샷 → 그 칸만 불일치), Spark 설정 단위 테스트 1개 추가, Spark 컨테이너 테스트 3개 통과 |

## 면접에서 나올 만한 질문

**Q. Spark 쪽은 JDBC 카탈로그, Python 쪽은 SQL 카탈로그인데 같은 것인가요?**
네, 같은 표를 씁니다. PyIceberg의 SqlCatalog은 Java JdbcCatalog과 같은 표 구조(`iceberg_tables`: 카탈로그 이름, 네임스페이스, 테이블 이름, 메타데이터 위치)를 쓰도록 만들어졌습니다. 그래서 카탈로그 이름(`lake`)과 스키마(`lakehouse`)만 맞추면 한쪽이 커밋한 포인터를 다른 쪽이 바로 봅니다. 실제로 PyIceberg가 만든 `gold.*`를 Spark가 읽었고, Spark가 만든 `spark.*`를 PyIceberg가 읽었습니다.

**Q. 왜 s3a(hadoop-aws)가 아니라 S3FileIO인가요?**
s3a는 Hadoop 파일시스템 계층을 거치고 hadoop-aws와 판을 맞춘 AWS SDK가 필요합니다. Iceberg의 S3FileIO는 테이블 메타데이터가 파일 목록을 알고 있다는 전제로 목록 조회 없이 객체를 직접 읽고 씁니다. 객체 저장소에 더 맞는 방식이라 Iceberg가 권장합니다.

**Q. 외부 jar를 이미지에 넣을 때 무엇을 확인했나요?**
받는 곳(Maven Central), 판, 크기, 배포일을 사람이 먼저 확인했습니다. 판과 SHA-512는 코드에 고정해, 빌드할 때마다 같은 바이트인지 확인합니다. Postgres 드라이버는 Maven에 SHA-512가 없어서, 공개된 SHA-1·MD5와 맞는 것을 확인한 파일에서 SHA-512를 계산해 고정했습니다(PGP 서명 검증은 하지 않음).

## 한계

- **Iceberg 1.12에서 Spark 읽기 옵션 `snapshot-id`가 없어져** SQL `VERSION AS OF`로 바꿨습니다([장애 기록](../interview/error_handling.md) 33번).
- **Spark 결과 테이블에는 처음 만들 때의 빈 스냅샷이 하나 있습니다**(내보내기 ID 없음). 시간 여행은 내보내기 ID로 찾으므로 영향이 없습니다.
- **옛 Parquet 결과(`spark/…/snapshot=…`)는 지우지 않았습니다.** 이제 새로 쓰지 않습니다.
- **jar 서명(PGP)은 검증하지 않습니다.** 고정한 해시로 같은 파일인지만 확인합니다.
