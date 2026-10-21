"""Spark 결과와 dbt 결과를 행 단위로 대조한다(로드맵 4a).

둘 다 같은 스냅샷(같은 입력)에서 나온 Parquet를 읽으므로, 내보낸 뒤 Gold가 다시 만들어져도 비교가 흔들리지 않는다.
대조는 Spark를 쓰지 않고 pyarrow로 한다. 검사하는 쪽이 검사받는 엔진에 기대지 않게 하기 위해서다.
허용 오차: dbt는 평균을 소수 넷째 자리, 쉰 날 수를 둘째 자리에서 반올림해 저장한다.
"""
import io
import json

import pyarrow.parquet as pq

from ..object_store import read_object
from .export import FEATURES, PREFIX
from .spark_features import OUTPUT_PREFIX

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


def same(column: str, left, right) -> bool:
    if left is None or right is None:
        return left is None and right is None
    if column.endswith("_n") or column.endswith("last_feature_kickoff"):
        return left == right
    return abs(float(left) - float(right)) <= TOLERANCE.get(column.split("_", 1)[1], DEFAULT_TOLERANCE)


def reconcile(store, bucket: str, snapshot: str | None = None) -> dict:
    """{rows_spark, rows_dbt, missing_in_spark, extra_in_spark, mismatches, examples}. 모두 0이고 행 수가 같아야 통과."""
    snapshot = snapshot or json.loads(read_object(store, bucket, f"{PREFIX}/latest.json"))["snapshot"]
    expected = {row["match_id"]: row for row in read_parquet(store, bucket, f"{PREFIX}/{snapshot}/dbt_match_features/")}
    actual = {row["match_id"]: row for row in read_parquet(store, bucket, f"{OUTPUT_PREFIX}/snapshot={snapshot}/")}
    columns = [f"{side}_{feature}" for side in ("home", "away") for feature in FEATURES]
    mismatches = [{"match_id": match_id, "column": column, "dbt": str(expected[match_id][column]),
                   "spark": str(actual[match_id][column])}
                  for match_id in sorted(expected.keys() & actual.keys()) for column in columns
                  if not same(column, expected[match_id][column], actual[match_id][column])]
    result = {"snapshot": snapshot, "rows_dbt": len(expected), "rows_spark": len(actual),
              "columns_compared": len(columns), "missing_in_spark": len(expected.keys() - actual.keys()),
              "extra_in_spark": len(actual.keys() - expected.keys()), "mismatches": len(mismatches),
              "examples": mismatches[:5]}
    result["passed"] = (result["missing_in_spark"] == result["extra_in_spark"] == result["mismatches"] == 0
                        and result["rows_dbt"] > 0)
    return result
