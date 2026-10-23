"""4a 레이크 테스트(pyspark 없이). 내보내기·대조는 Postgres·S3·dbt가 있을 때만, Parquet 변환은 단위 테스트."""
import io
import json
from dataclasses import replace
from datetime import datetime, timezone

import psycopg
import pyarrow as pa
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


def test_run_isolated_reports_success_and_fails_loudly(monkeypatch):
    import subprocess

    from pipelines.lake import run

    calls = []

    def fake(returncode, stdout, stderr=""):
        def runner(command, **kwargs):
            calls.append(command)
            return subprocess.CompletedProcess(command, returncode, stdout, stderr)
        return runner

    ok = {"export": {"snapshot": "s"}, "reconcile": {"passed": True, "mismatches": 0}}
    monkeypatch.setattr(run.subprocess, "run", fake(0, json.dumps(ok)))
    assert run.run_isolated("all", python="/spark/python") == ok
    assert calls[-1] == ["/spark/python", "-m", "pipelines.lake.run", "all"]

    mismatch = {"reconcile": {"passed": False, "mismatches": 3}}
    monkeypatch.setattr(run.subprocess, "run", fake(1, json.dumps(mismatch)))
    with pytest.raises(RuntimeError, match="mismatches"):   # 대조 불일치 → Airflow 작업 실패
        run.run_isolated("all", python="/spark/python")

    monkeypatch.setattr(run.subprocess, "run", fake(1, "", "Java gateway process exited"))
    with pytest.raises(RuntimeError, match="Java gateway"):   # Spark가 아예 못 뜬 경우도 원인을 보여 준다
        run.run_isolated("all", python="/spark/python")


def test_nba_standings_reconcile_detects_differences(lake):
    """6b: NBA 순위표 대조. dbt 결과를 Spark 자리에 두면 통과, 순위 하나를 바꾸면 그 칸만 불일치."""
    from datetime import timezone
    from pipelines.ingest import balldontlie
    from pipelines.lake.spark_features import NBA_OUTPUT_PREFIX
    from tests.test_balldontlie import CELTICS, KNICKS, game, page
    s, store = lake
    balldontlie.ingest("2025-26", pages=[page([game(1, home=CELTICS, away=KNICKS), game(2, home=KNICKS, away=CELTICS,
                                                                                    home_score=101, away_score=99, day="2025-10-25")])],
                       settings=s, received_at=datetime(2025, 10, 27, tzinfo=timezone.utc))
    build(s)
    with psycopg.connect(s.postgres_conninfo) as conn:
        manifest = export.export(conn, store, TEST_LAKE_BUCKET)
    snapshot = manifest["snapshot"]
    assert manifest["tables"]["nba_games"]["rows"] == 2 and manifest["tables"]["dbt_nba_standings"]["rows"] == 2
    key = manifest["tables"]["dbt_nba_standings"]["files"][0]["key"]
    payload = store.get_object(Bucket=TEST_LAKE_BUCKET, Key=key)["Body"].read()
    target = f"{NBA_OUTPUT_PREFIX}/snapshot={snapshot}/part-0.parquet"

    def as_spark_output(rows):
        """Spark 결과처럼 시즌을 파일 안의 열로(dbt 내보내기는 시즌을 경로 season=…에 둔다)."""
        import pyarrow as pa
        schema = pa.schema([("season", pa.string()), *export.TABLES["dbt_nba_standings"][1]])
        return export.to_parquet([("2025-26", *row.values()) for row in rows], schema)

    table = pq.read_table(io.BytesIO(payload)).to_pylist()
    store.put_object(Bucket=TEST_LAKE_BUCKET, Key=target, Body=as_spark_output(table))
    result = reconcile.reconcile_nba(store, TEST_LAKE_BUCKET, snapshot)
    assert result["passed"] and result["rows_dbt"] == result["rows_spark"] == 2 and result["columns_compared"] == 9

    table[0]["position"], table[1]["position"] = table[1]["position"], table[0]["position"]   # 순위를 뒤바꿈
    store.put_object(Bucket=TEST_LAKE_BUCKET, Key=target, Body=as_spark_output(table))
    result = reconcile.reconcile_nba(store, TEST_LAKE_BUCKET, snapshot)
    assert not result["passed"] and result["mismatches"] == 2 and {e["column"] for e in result["examples"]} == {"position"}


def test_iceberg_snapshots_time_travel_and_agree_with_dbt_as_of(lake):
    """7a: 내보내기마다 Iceberg 스냅샷 하나, 같은 내보내기는 다시 쓰지 않음, 옛 스냅샷 = 그 시점으로 다시 계산한 dbt Gold."""
    from pipelines.lake import iceberg
    s, store = lake
    with psycopg.connect(s.postgres_conninfo) as conn:
        first = export.export(conn, store, TEST_LAKE_BUCKET)
    published = iceberg.publish(s, store, TEST_LAKE_BUCKET, first["snapshot"])
    assert all(table["written"] for table in published["tables"].values())
    assert published["tables"]["team_match_results"]["rows"] == first["tables"]["team_match_results"]["rows"]
    again = iceberg.publish(s, store, TEST_LAKE_BUCKET, first["snapshot"])
    assert not any(table["written"] for table in again["tables"].values())   # 멱등
    between = datetime.now(timezone.utc)

    # 이틀 뒤 소스가 첫 경기 점수를 정정(Alpha 2-1 → 3-1)했다고 치고 다시 만들어 내보낸다.
    corrected = json.loads(SAMPLE_PATH.read_bytes())
    corrected["matches"][0]["score"]["ft"] = [3, 1]
    ingest(s, json.dumps(corrected).encode(), "2025-08-19T00:00:00+00:00")
    build(s)
    with psycopg.connect(s.postgres_conninfo) as conn:
        second = export.export(conn, store, TEST_LAKE_BUCKET)
    assert second["snapshot"] != first["snapshot"]
    iceberg.publish(s, store, TEST_LAKE_BUCKET, second["snapshot"])

    alpha = lambda data: sorted(row["goals_for"] for row in data.to_pylist() if row["team"] == "Alpha FC")
    assert alpha(iceberg.read(s, TEST_LAKE_BUCKET, "team_match_results")) == [3]
    old = iceberg.read(s, TEST_LAKE_BUCKET, "team_match_results", export=first["snapshot"])
    assert alpha(old) == [2]
    assert alpha(iceberg.read(s, TEST_LAKE_BUCKET, "team_match_results", as_of=between)) == [2]   # 시각으로 시간 여행

    # 두 시간 여행이 같은 답: Postgres 기준 시각으로 다시 계산한 Gold = 그때 쓴 Iceberg 스냅샷
    build(s, "--vars", '{as_of: "2025-08-18T00:00:00Z"}')
    with psycopg.connect(s.postgres_conninfo) as conn:
        rebuilt = sorted(conn.execute(export.TABLES["team_match_results"][0]).fetchall())
    assert rebuilt == sorted(tuple(row.values()) for row in old.to_pylist())

    table = iceberg.catalog(s, TEST_LAKE_BUCKET).load_table("gold.team_match_results")
    assert [item["export"] for item in iceberg.history(table)] == [first["snapshot"], second["snapshot"]]
    assert table.spec().fields[0].name == "season"
    report = iceberg.verify(s, store, TEST_LAKE_BUCKET)
    assert report["passed"] and report["snapshots_checked"] == 2 * len(export.TABLES)

    # 검사가 실제로 잡는지: 내보내기와 다른 내용을 그 내보내기 ID로 써 넣으면 불일치
    current = table.scan().to_arrow().to_pylist()
    current[0]["goals_for"] += 1
    table.overwrite(pa.Table.from_pylist(current, schema=iceberg.arrow_schema("team_match_results")),
                    snapshot_properties={iceberg.SNAPSHOT_PROPERTY: second["snapshot"]})
    report = iceberg.verify(s, store, TEST_LAKE_BUCKET)
    assert not report["passed"] and report["mismatches"] == [
        {"table": "team_match_results", "export": second["snapshot"], "iceberg_rows": 6, "export_rows": 6}]   # 행 수는 같고 값 하나만 다름
