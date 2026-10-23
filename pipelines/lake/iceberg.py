"""Iceberg 레이크하우스(로드맵 7a): Parquet 내보내기를 Iceberg 테이블의 스냅샷으로 쌓는다.

4a의 내보내기는 exports/<스냅샷>/<테이블>/season=…/part-0.parquet 파일 묶음이다. 파일은 남지만 "어느 묶음이 언제의
테이블이었나"는 latest.json 하나뿐이라, 과거 상태를 보려면 스냅샷 ID를 따로 알아야 했다. 여기서는 같은 내용을
Iceberg 테이블(gold.<테이블>)에 쓴다.
- 내보내기 한 번 = 테이블마다 Iceberg 스냅샷 하나(전체 덮어쓰기). 스냅샷 요약에 내보내기 ID(export.snapshot)를 남긴다.
  같은 내보내기를 다시 쓰려 하면 건너뛴다(멱등).
- 시간 여행: 내보내기 ID 또는 시각으로 그때의 테이블을 읽는다(read). Postgres의 기준 시각 조회(*_known_at(as_of))와
  같은 생각을 레이크 쪽에서 파일 형식으로 한다.
- 카탈로그: PyIceberg SqlCatalog. 포인터는 Postgres lakehouse 스키마(019), 파일은 lake 버킷 iceberg/ 아래.
  Spark·Trino 등 다른 엔진도 같은 카탈로그·파일을 읽을 수 있다.
- 시즌별 파티션(identity). 시즌 열은 내보내기 경로(season=…)에서 가져와 열로 넣는다.
- 7b부터 Spark도 같은 카탈로그(Iceberg JDBC 카탈로그)로 gold.* 를 그 내보내기의 스냅샷으로 읽고, 결과를 spark.* 에 쓴다.
검사(verify): 각 Iceberg 스냅샷을 그 스냅샷이 가리키는 내보내기 파일과 행 단위로 맞춰 본다.
"""
import json
from datetime import datetime, timezone

import pyarrow as pa

from ..object_store import read_object
from .export import PREFIX, TABLES
from .reconcile import read_parquet

NAMESPACE = "gold"
SPARK_NAMESPACE = "spark"   # Spark 결과(7b): spark.match_features_pit, spark.nba_standings
CATALOG_NAME = "lake"       # PyIceberg와 Spark가 같은 이름을 써야 같은 카탈로그 행(catalog_name)을 본다
WAREHOUSE_PREFIX = "iceberg"
SNAPSHOT_PROPERTY = "export.snapshot"


def catalog(settings, bucket: str | None = None):
    """Postgres를 카탈로그로, lake 버킷을 저장소로 쓰는 PyIceberg 카탈로그."""
    from pyiceberg.catalog.sql import SqlCatalog
    from sqlalchemy.engine import URL
    uri = URL.create("postgresql+psycopg", username=settings.postgres_user, password=settings.postgres_password,
                     host=settings.postgres_host, port=settings.postgres_port, database=settings.postgres_db,
                     query={"options": "-csearch_path=lakehouse"}).render_as_string(hide_password=False)
    return SqlCatalog(CATALOG_NAME, uri=uri, warehouse=f"s3://{bucket or settings.lake_bucket}/{WAREHOUSE_PREFIX}", **{
        "s3.endpoint": settings.s3_endpoint_url, "s3.access-key-id": settings.s3_access_key,
        "s3.secret-access-key": settings.s3_secret_key, "s3.region": "us-east-1",
        "s3.force-virtual-addressing": "false"})


def arrow_schema(name: str) -> pa.Schema:
    """내보내기 파일의 열 + 맨 앞의 시즌(경로에서 온 값)."""
    return pa.schema([pa.field("season", pa.string(), nullable=False), *TABLES[name][1]])


def export_table(store, bucket: str, snapshot: str, name: str) -> pa.Table:
    """내보내기 스냅샷의 한 테이블을 시즌 열을 붙인 Arrow 테이블로."""
    schema = arrow_schema(name)
    rows = read_parquet(store, bucket, f"{PREFIX}/{snapshot}/{name}/")
    return pa.Table.from_pylist([{field.name: row.get(field.name) for field in schema} for row in rows], schema=schema)


def ensure_table(cat, name: str):
    from pyiceberg.exceptions import NoSuchTableError
    cat.create_namespace_if_not_exists(NAMESPACE)
    identifier = f"{NAMESPACE}.{name}"
    try:
        return cat.load_table(identifier)
    except NoSuchTableError:
        table = cat.create_table(identifier, schema=arrow_schema(name),
                                 properties={"write.parquet.compression-codec": "zstd"})
        with table.update_spec() as spec:
            spec.add_identity("season")
        return cat.load_table(identifier)


def history(table) -> list[dict]:
    """내보내기마다 완성된 스냅샷 하나(오래된 순): Iceberg 스냅샷 ID, 시각, 내보내기 ID, 행 수.

    비어 있지 않은 테이블을 덮어쓰면 Iceberg는 스냅샷 두 개(지우기 → 넣기)를 만들고 둘 다 같은 요약 속성을 단다.
    지우기만 끝난 중간 스냅샷은 빈 테이블이라, 내보내기마다 마지막 스냅샷만 남긴다.
    """
    found = {}
    for entry in table.history():
        snapshot = table.snapshot_by_id(entry.snapshot_id)
        summary = snapshot.summary
        export_id = summary.get(SNAPSHOT_PROPERTY)
        found.pop(export_id, None)   # 같은 내보내기의 앞 스냅샷을 지우고 뒤로 보낸다(순서 유지)
        found[export_id] = {"snapshot_id": snapshot.snapshot_id,
                            "committed_at": datetime.fromtimestamp(snapshot.timestamp_ms / 1000, timezone.utc).isoformat(),
                            "export": export_id, "rows": int(summary.get("total-records", 0))}
    return list(found.values())


def snapshot_for_export(table, export: str) -> int:
    """그 내보내기를 쓴 Iceberg 스냅샷 ID. 없으면 LookupError(다른 스냅샷으로 조용히 대신하지 않는다)."""
    matches = [item["snapshot_id"] for item in history(table) if item["export"] == export]
    if not matches:
        raise LookupError(f"{table.name()}: 내보내기 {export}를 쓴 스냅샷이 없습니다.")
    return matches[-1]


def write_snapshot(table, data: pa.Table, export: str) -> int:
    """테이블 전체를 data로 바꾸는 스냅샷을 커밋하고 그 ID를 돌려준다. 요약에 내보내기 ID를 남긴다."""
    properties = {SNAPSHOT_PROPERTY: export}
    current = table.current_snapshot()
    if current is None or int(current.summary.get("total-records", 0)) == 0:
        table.append(data, snapshot_properties=properties)      # 지울 것이 없다(첫 쓰기·빈 테이블)
    else:
        table.overwrite(data, snapshot_properties=properties)   # 전체 덮어쓰기(지우기 + 넣기 스냅샷)
    return table.current_snapshot().snapshot_id


def publish(settings, store, bucket: str, snapshot: str | None = None) -> dict:
    """내보내기 스냅샷을 테이블마다 Iceberg 스냅샷으로 쓴다. 이미 쓴 내보내기면 그 테이블은 건너뛴다."""
    snapshot = snapshot or json.loads(read_object(store, bucket, f"{PREFIX}/latest.json"))["snapshot"]
    cat = catalog(settings, bucket)
    result = {"export": snapshot, "tables": {}}
    for name in TABLES:
        table = ensure_table(cat, name)
        if any(item["export"] == snapshot for item in history(table)):
            result["tables"][name] = {"written": False, "reason": "ALREADY_PUBLISHED"}
            continue
        data = export_table(store, bucket, snapshot, name)
        result["tables"][name] = {"written": True, "rows": data.num_rows,
                                  "iceberg_snapshot": write_snapshot(table, data, snapshot)}
    return result


def read(settings, bucket: str, name: str, *, export: str | None = None, as_of: datetime | None = None,
         namespace: str = NAMESPACE) -> pa.Table:
    """시간 여행 읽기. export(내보내기 ID) 또는 as_of(그 시각에 최신이던 스냅샷)를 주지 않으면 지금 테이블."""
    table = catalog(settings, bucket).load_table(f"{namespace}.{name}")
    snapshot_id = None
    if export is not None:
        snapshot_id = snapshot_for_export(table, export)
    elif as_of is not None:
        snapshot = table.snapshot_as_of_timestamp(int(as_of.timestamp() * 1000))
        if snapshot is None:
            raise LookupError(f"{name}: {as_of.isoformat()}에는 아직 테이블이 비어 있었습니다.")
        snapshot_id = snapshot.snapshot_id
    return table.scan(snapshot_id=snapshot_id).to_arrow()


def _rows(data: pa.Table) -> list[tuple]:
    """열 이름 순으로 맞춘 행 목록(순서 무관 비교용). 결측(None)이 섞여도 정렬되게 repr로 정렬한다."""
    return sorted((tuple(row.values()) for row in data.select(sorted(data.column_names)).to_pylist()), key=repr)


def verify(settings, store, bucket: str) -> dict:
    """모든 테이블의 모든 Iceberg 스냅샷을 그 내보내기 파일과 행 단위로 비교한다. 하나라도 다르면 passed=False."""
    cat = catalog(settings, bucket)
    checked, mismatches = 0, []
    for name in TABLES:
        table = ensure_table(cat, name)
        for item in history(table):
            if item["export"] is None:
                continue
            iceberg_rows = _rows(table.scan(snapshot_id=item["snapshot_id"]).to_arrow())
            export_rows = _rows(export_table(store, bucket, item["export"], name))
            checked += 1
            if iceberg_rows != export_rows:
                mismatches.append({"table": name, "export": item["export"], "iceberg_rows": len(iceberg_rows),
                                   "export_rows": len(export_rows)})
    return {"snapshots_checked": checked, "mismatches": mismatches, "passed": not mismatches}
