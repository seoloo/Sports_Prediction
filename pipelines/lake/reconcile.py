"""Spark 결과와 dbt 결과를 행 단위로 대조한다(로드맵 4a).

둘 다 같은 내보내기(같은 입력)에서 나온 결과를 읽으므로, 내보낸 뒤 Gold가 다시 만들어져도 비교가 흔들리지 않는다.
정답(dbt)은 내보내기 Parquet, Spark 결과는 7b부터 Iceberg 테이블 spark.*에서 그 내보내기 ID를 단 스냅샷을 읽는다.
대조는 Spark를 쓰지 않고 pyarrow로 한다. 검사하는 쪽이 검사받는 엔진에 기대지 않게 하기 위해서다.
허용 오차: dbt는 평균을 소수 넷째 자리, 쉰 날 수를 둘째 자리에서 반올림해 저장한다.
"""
import io
import json

import pyarrow.parquet as pq

from ..object_store import read_object
from .export import FEATURES, NBA_COUNTS, PREFIX
from .spark_features import FEATURES_TABLE, NBA_TABLE

TOLERANCE = {"rest_days": 0.006}   # 둘째 자리 반올림(0.005) + 부동소수 여유
DEFAULT_TOLERANCE = 0.00006         # 넷째 자리 반올림(0.00005) + 여유


def read_parquet(store, bucket: str, prefix: str) -> list[dict]:
    """prefix 아래 Parquet 파일을 모두 읽어 행 목록으로. Hive 방식 경로(season=…)는 열로 붙인다."""
    rows = []
    for page in store.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            if not item["Key"].endswith(".parquet"):
                continue
            partition = {part.split("=", 1)[0]: part.split("=", 1)[1] for part in item["Key"].split("/") if "=" in part}
            table = pq.read_table(io.BytesIO(read_object(store, bucket, item["Key"])))
            rows += [{**partition, **row} for row in table.to_pylist()]
    return rows


def spark_rows(settings, bucket: str, name: str, snapshot: str) -> list[dict]:
    """Spark 결과 테이블(spark.<name>)에서 그 내보내기의 스냅샷. 테이블이나 스냅샷이 없으면 빈 목록(→ 대조 실패로 드러남)."""
    from pyiceberg.exceptions import NoSuchTableError
    from . import iceberg
    try:
        return iceberg.read(settings, bucket, name, export=snapshot, namespace=iceberg.SPARK_NAMESPACE).to_pylist()
    except (LookupError, NoSuchTableError):
        return []


def same(column: str, left, right) -> bool:
    if left is None or right is None:
        return left is None and right is None
    if column.endswith("_n") or column.endswith("last_feature_kickoff"):
        return left == right
    return abs(float(left) - float(right)) <= TOLERANCE.get(column.split("_", 1)[1], DEFAULT_TOLERANCE)


def reconcile(store, bucket: str, snapshot: str | None = None, settings=None) -> dict:
    """{rows_spark, rows_dbt, missing_in_spark, extra_in_spark, mismatches, examples}. 모두 0이고 행 수가 같아야 통과."""
    from ..config import load_settings
    settings = settings or load_settings()
    snapshot = snapshot or json.loads(read_object(store, bucket, f"{PREFIX}/latest.json"))["snapshot"]
    expected = {row["match_id"]: row for row in read_parquet(store, bucket, f"{PREFIX}/{snapshot}/dbt_match_features/")}
    actual = {row["match_id"]: row for row in spark_rows(settings, bucket, FEATURES_TABLE, snapshot)}
    columns = [f"{side}_{feature}" for side in ("home", "away") for feature in FEATURES]
    mismatches = [{"match_id": match_id, "column": column, "dbt": str(expected[match_id][column]),
                   "spark": str(actual[match_id][column])}
                  for match_id in sorted(expected.keys() & actual.keys()) for column in columns
                  if not same(column, expected[match_id][column], actual[match_id][column])]
    result = {"snapshot": snapshot, "rows_dbt": len(expected), "rows_spark": len(actual),
              "columns_compared": len(columns), "missing_in_spark": len(expected.keys() - actual.keys()),
              "extra_in_spark": len(actual.keys() - expected.keys()), "mismatches": len(mismatches),
              "examples": mismatches[:5]}
    result["nba"] = reconcile_nba(store, bucket, snapshot, settings)
    result["passed"] = (result["missing_in_spark"] == result["extra_in_spark"] == result["mismatches"] == 0
                        and result["rows_dbt"] > 0 and result["nba"]["passed"])
    return result


NBA_TOLERANCE = {"win_pct": 0.00006, "avg_margin": 0.006}   # dbt는 승률을 넷째, 평균 득실 차를 둘째 자리에서 반올림


def reconcile_nba(store, bucket: str, snapshot: str, settings) -> dict:
    """NBA 순위표(6b): dbt ↔ Spark를 (시즌, 팀) 단위로 모든 열 대조. 양쪽 다 NBA 자료가 없으면 통과(축구만 있는 환경)."""
    key = lambda row: (row["season"], row["team"])
    expected = {key(row): row for row in read_parquet(store, bucket, f"{PREFIX}/{snapshot}/dbt_nba_standings/")}
    actual = {key(row): row for row in spark_rows(settings, bucket, NBA_TABLE, snapshot)}
    columns = [*NBA_COUNTS, "win_pct", "avg_margin", "position"]

    def equal(column, left, right):
        if column in NBA_TOLERANCE:
            return abs(float(left) - float(right)) <= NBA_TOLERANCE[column]
        return int(left) == int(right)

    mismatches = [{"season": k[0], "team": k[1], "column": column, "dbt": str(expected[k][column]), "spark": str(actual[k][column])}
                  for k in sorted(expected.keys() & actual.keys()) for column in columns
                  if not equal(column, expected[k][column], actual[k][column])]
    result = {"rows_dbt": len(expected), "rows_spark": len(actual), "columns_compared": len(columns),
              "missing_in_spark": len(expected.keys() - actual.keys()), "extra_in_spark": len(actual.keys() - expected.keys()),
              "mismatches": len(mismatches), "examples": mismatches[:5]}
    result["passed"] = result["missing_in_spark"] == result["extra_in_spark"] == result["mismatches"] == 0
    return result
