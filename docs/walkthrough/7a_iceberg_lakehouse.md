# 7a. Iceberg 레이크하우스: 내보내기를 테이블 스냅샷으로

작성: 2026-10-24. 상태: **완료.** 7단계(레이크하우스·운영)의 첫 단계입니다.

## 한 줄 요약

4a부터 매일 Gold를 Parquet 파일 묶음(`exports/<내용 해시>/…`)으로 내보내 왔습니다. 이번에는 그 묶음을 **Apache Iceberg 테이블 5개(`gold.*`)의 스냅샷**으로 쌓습니다. 내보내기 한 번이 테이블마다 Iceberg 스냅샷 하나가 되고, 스냅샷에 내보내기 ID를 붙여 둡니다. 그래서 "그때의 테이블"을 **내보내기 ID나 시각으로 바로 읽을 수 있습니다(시간 여행)**.

테스트에서는 두 가지 시간 여행이 같은 답을 내는지 확인했습니다. 하나는 Postgres의 기준 시각 조회로 dbt Gold를 그 시점으로 다시 계산한 결과, 다른 하나는 그때 써 둔 Iceberg 스냅샷입니다. 둘은 행 단위로 같았습니다.

## 흐름

```text
매일 DAG의 spark_reconcile 작업(Airflow 안 Spark 가상환경) = python -m pipelines.lake.run all
  export   Gold → exports/<내보내기 ID>/<테이블>/season=…/part-0.parquet (4a, 그대로)
  iceberg  publish: 내보내기 ID가 아직 없는 테이블만 Iceberg 스냅샷으로 쓰기(첫 쓰기는 append, 그 뒤는 전체 overwrite)
           verify : 모든 테이블의 모든 스냅샷을 그 내보내기 파일과 행 단위로 대조 → 하나라도 다르면 작업 실패
  spark    특징·NBA 순위표 재계산(4a·6b, 그대로)
  reconcile Spark ↔ dbt 대조(그대로)

카탈로그: Postgres lakehouse 스키마(iceberg_tables: 테이블 → 지금 메타데이터 파일 위치)
파일:     s3://lake/iceberg/gold/<테이블>/{metadata, data/season=…}
```

## 왜 Iceberg인가(파일 묶음과 무엇이 다른가)

| | Parquet 파일 묶음(4a) | Iceberg 테이블(7a) |
|---|---|---|
| "지금 테이블" | `latest.json`이 가리키는 묶음 | 카탈로그의 현재 메타데이터 |
| 과거 상태 | 내보내기 ID를 따로 알아야 함 | 스냅샷 목록에서 ID·시각으로 고름(`history`, `read(as_of=…)`) |
| 쓰는 중에 읽기 | 파일을 다 올리기 전이면 반쪽을 볼 수 있음 | 메타데이터 포인터 교체가 한 번에 일어남(원자적 커밋) |
| 스키마·파티션 | 경로 규칙과 코드에만 있음 | 테이블 메타데이터에 기록(시즌 identity 파티션) |
| 다른 엔진 | 경로 규칙을 알아야 읽음 | Spark·Trino·DuckDB 등이 같은 카탈로그로 읽음 |

Postgres Silver의 버전 테이블은 그대로입니다. Iceberg는 "발표 시각·수신 시각" 두 축을 대신하지 않습니다. 내보낸 Gold(한 축: 내보낸 시각)를 레이크 쪽에서 시점 조회할 수 있게 한 것입니다.

## 바꾼 것

| 변경 | 위치 | 이유 |
|---|---|---|
| Iceberg 쓰기·읽기·검사 | `pipelines/lake/iceberg.py`(`publish`, `read`, `history`, `verify`) | 내보내기를 바꾸지 않고 그 위에 테이블 형식을 얹음. 입력이 같은 파일이라 내용이 같아야 함 |
| 카탈로그 스키마 | `pipelines/sql/019_lakehouse.sql` | 카탈로그 표를 public에 섞지 않고 마이그레이션으로 기록 |
| 실행 단계 | `pipelines/lake/run.py`의 `iceberg`, `all` | 매일 DAG가 자동으로 쓰고 검사. 검사 실패면 종료 코드 1 → 작업 실패 → 텔레그램 |
| 의존성 | `pyiceberg 0.12.0`, `SQLAlchemy 2.1.4`(PC·CI·Spark 이미지·Airflow의 Spark 가상환경) | psycopg2를 따로 넣지 않고 기존 psycopg 3를 SQLAlchemy로 씀. 나온 지 2주가 안 된 하위 의존성 4개는 그 전 판으로 고정 |

## 사용한 기술

| 기술 | 어디에 | 어떻게 썼나 |
|---|---|---|
| Apache Iceberg 테이블 형식 + PyIceberg | `iceberg.py` | 스냅샷마다 메타데이터·매니페스트를 남기고, 카탈로그 포인터 교체로 커밋 |
| SQL 카탈로그(PyIceberg `SqlCatalog`) | Postgres `lakehouse` 스키마 | REST·Hive 카탈로그 서버 없이 이미 있는 DB로. 접속 주소에 `search_path=lakehouse` |
| 스냅샷 요약 속성(`snapshot_properties`) | `export.snapshot` | Iceberg 스냅샷 ↔ 내보내기 ID 연결. 같은 ID가 있으면 쓰지 않음(멱등) |
| 시간 여행(`scan(snapshot_id=…)`, `snapshot_as_of_timestamp`) | `read` | 내보내기 ID 또는 시각으로 그때의 테이블 |
| 파티션 진화(`update_spec().add_identity`) | `ensure_table` | 테이블을 만든 뒤 시즌 파티션을 얹음 |
| pyarrow S3 파일 입출력 | 카탈로그 설정(`s3.endpoint`, 경로 방식 주소) | SeaweedFS에 그대로. 코드는 표준 S3라 AWS로 옮겨도 같음 |

## 확인 결과(2026-10-24)

| 확인 | 결과 |
|---|---|
| 실제 데이터(Spark 컨테이너, `run all`) | 내보내기 `1d01414f29fc689e` → Iceberg 5개 테이블(21,710 / 12,264 / 12,264 / 10,245 / 270행), 검사 5개 스냅샷 불일치 0, Spark 대조도 그대로 통과 |
| 다시 실행(PC) | 5개 모두 `ALREADY_PUBLISHED`, 새 스냅샷 0. 컨테이너가 쓴 테이블을 PC에서 그대로 읽음 |
| 통합 테스트 | 내보내기 2번(점수 정정 전후) → 스냅샷 2개, 현재는 정정 값, 내보내기 ID·시각으로 읽으면 정정 전 값, **dbt를 `as_of`로 다시 계산한 Gold = 정정 전 Iceberg 스냅샷**, 값 하나를 바꿔 써 넣으면 검사가 그 테이블·내보내기를 불일치로 잡음 |

## 면접에서 나올 만한 질문

**Q. 이미 Postgres에 시점 조회가 있는데 Iceberg가 왜 필요한가요?**
둘은 다른 시간을 다룹니다. Postgres는 "출처가 언제 발표했고 우리가 언제 받았나"(두 축)로 Silver를 버전 관리하고, 어떤 기준 시각으로든 Gold를 다시 계산합니다. Iceberg는 "그날 내보낸 Gold가 무엇이었나"를 계산 없이 파일로 남깁니다. 레이크를 읽는 쪽(Spark, 분석 도구)은 Postgres 없이도 과거 상태를 봅니다. 테스트에서 두 답이 같은지 확인한 것이 이 단계의 핵심입니다.

**Q. 카탈로그는 왜 SQL 카탈로그인가요?**
REST 카탈로그(Polaris·Nessie 등)나 Hive 메타스토어는 서버를 하나 더 띄워야 합니다. 카탈로그가 하는 일은 "테이블 → 현재 메타데이터 파일" 포인터를 트랜잭션으로 바꾸는 것이고, 이미 Postgres가 있습니다. 엔진이 늘어 접근 제어나 여러 카탈로그가 필요해지면 REST로 옮길 수 있습니다(테이블 파일은 그대로).

**Q. overwrite가 스냅샷을 두 개 만든다는 건 무슨 뜻인가요?**
비어 있지 않은 테이블을 덮어쓰면 PyIceberg는 "지우기" 스냅샷과 "넣기" 스냅샷을 차례로 커밋하고, 둘 다 같은 요약 속성을 답니다. 처음에는 스냅샷 목록에 같은 내보내기가 두 번 나와 테스트가 실패했습니다. 지우기만 끝난 중간 스냅샷은 빈 테이블이라 시간 여행 대상이 아니므로, 내보내기마다 마지막 스냅샷만 남깁니다([장애 기록](../interview/error_handling.md) 32번).

## 한계

- **Spark는 아직 Iceberg가 아니라 내보내기 파일을 읽습니다.** Spark가 Iceberg를 직접 읽으려면 `iceberg-spark-runtime`과 AWS 번들 jar가 필요합니다(다음 단계).
- **스냅샷 정리를 하지 않습니다.** 하루 한 번, 내용이 바뀔 때만 스냅샷이 생겨 지금은 작습니다. 오래된 스냅샷 만료(`expire_snapshots`)는 운영 단계에서 기준을 정합니다.
- **전체 덮어쓰기입니다.** 행 단위 병합(MERGE)이 아니라 내보내기마다 테이블 전체를 씁니다. 자료가 수만 행이라 단순한 쪽을 택했습니다.
