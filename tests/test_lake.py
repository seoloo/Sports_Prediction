"""4a 레이크 테스트(pyspark 없이). 내보내기·대조는 Postgres·S3·dbt가 있을 때만, Parquet 변환은 단위 테스트."""
import io
import json
from dataclasses import replace
from datetime import datetime, timezone

import psycopg
import pyarrow.parquet as pq
import pytest

from pipelines.lake import export, reconcile
from pipelines.lake.spark_features import OUTPUT_PREFIX
from pipelines.object_store import client, put_if_absent
from tests.test_dbt_integration import build, dbt_settings, ingest  # noqa: F401 (fixture 재사용)
from tests.test_pipeline_integration import SAMPLE_PATH, empty_bucket, isolated_settings  # noqa: F401

TEST_LAKE_BUCKET = "lake-integration-test"   # 버킷을 테스트마다 새로 만들면 SeaweedFS 볼륨을 소모하므로 하나를 재사용


def test_snapshot_id_depends_only_on_content():
    rows = {"t": [(1, datetime(2025, 8, 16, 14, tzinfo=timezone.utc), "A")]}
    assert export.snapshot_id(rows) == export.snapshot_id({"t": list(rows["t"])})
    assert export.snapshot_id(rows) != export.snapshot_id({"t": [(1, datetime(2025, 8, 16, 15, tzinfo=timezone.utc), "A")]})


def test_parquet_keeps_types_and_utc_times():
    schema = export.TABLES["team_match_results"][1]
    kickoff = datetime(2025, 8, 16, 14, tzinfo=timezone.utc)
    table = pq.read_table(io.BytesIO(export.to_parquet([("Alpha FC", 7, kickoff, 2, 1, 3)], schema)))
    assert table.schema == schema and table.to_pylist() == [
        {"team": "Alpha FC", "match_id": 7, "kickoff_utc": kickoff, "goals_for": 2, "goals_against": 1, "points": 3}]
    assert pq.read_table(io.BytesIO(export.to_parquet([], schema))).num_rows == 0


def test_reconcile_tolerances():
    assert reconcile.same("home_l10_gf", 1.23455, 1.2345) and not reconcile.same("home_l10_gf", 1.2347, 1.2345)
    assert reconcile.same("away_rest_days", 6.995, 7.0) and not reconcile.same("away_rest_days", 6.98, 7.0)
    assert reconcile.same("home_l10_n", 10, 10) and not reconcile.same("home_l10_n", 9, 10)
    assert reconcile.same("home_l10_gf", None, None) and not reconcile.same("home_l10_gf", None, 0.0)


@pytest.fixture
def lake(dbt_settings):
    s = replace(dbt_settings, lake_bucket=TEST_LAKE_BUCKET)
    store = client(s)
    empty_bucket(store, TEST_LAKE_BUCKET)
    ingest(s, SAMPLE_PATH.read_bytes(), "2025-08-17T00:00:00+00:00")
    build(s)
    yield s, store
    empty_bucket(store, TEST_LAKE_BUCKET)


def test_export_is_idempotent_and_reconcile_detects_differences(lake):
    s, store = lake
    with psycopg.connect(s.postgres_conninfo) as conn:
        first = export.export(conn, store, TEST_LAKE_BUCKET)
        again = export.export(conn, store, TEST_LAKE_BUCKET)
    snapshot = first["snapshot"]
    assert again["snapshot"] == snapshot and first["uploaded_files"] == 3 and again["uploaded_files"] == 0
    assert first["tables"]["matches"]["rows"] == 4 and first["tables"]["dbt_match_features"]["rows"] == 4
    assert json.loads(store.get_object(Bucket=TEST_LAKE_BUCKET, Key="exports/latest.json")["Body"].read()) == {
        "snapshot": snapshot}

    # Spark 결과 자리에 dbt 결과를 그대로 두면 통과해야 한다.
    expected_key = first["tables"]["dbt_match_features"]["files"][0]["key"]
    payload = store.get_object(Bucket=TEST_LAKE_BUCKET, Key=expected_key)["Body"].read()
    put_if_absent(store, TEST_LAKE_BUCKET, f"{OUTPUT_PREFIX}/snapshot={snapshot}/part-0.parquet", payload, {})
    result = reconcile.reconcile(store, TEST_LAKE_BUCKET)
    assert result["passed"] and result["rows_dbt"] == result["rows_spark"] == 4 and result["columns_compared"] == 18

    # 값 하나를 바꾸면 그 칸만 불일치로 잡아야 한다.
    table = pq.read_table(io.BytesIO(payload)).to_pylist()
    changed = next(row for row in table if row["home_l10_n"])
    changed["home_l10_gf"] += 0.01
    broken = export.to_parquet([tuple(row.values()) for row in table], export.TABLES["dbt_match_features"][1])
    store.put_object(Bucket=TEST_LAKE_BUCKET, Key=f"{OUTPUT_PREFIX}/snapshot={snapshot}/part-0.parquet", Body=broken)
    result = reconcile.reconcile(store, TEST_LAKE_BUCKET, snapshot)
    assert not result["passed"] and result["mismatches"] == 1
    assert result["examples"][0]["column"] == "home_l10_gf" and result["examples"][0]["match_id"] == changed["match_id"]
