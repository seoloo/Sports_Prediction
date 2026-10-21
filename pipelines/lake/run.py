"""레이크 단계 실행(로드맵 4a): export → spark → reconcile.

python -m pipelines.lake.run export      Gold → Parquet(lake 버킷). 호스트에서도 된다
python -m pipelines.lake.run spark       Spark로 특징 재계산. pyspark가 있는 컨테이너에서만
python -m pipelines.lake.run reconcile   Spark 결과 ↔ dbt 결과 대조. 호스트에서도 된다
python -m pipelines.lake.run all         세 단계를 차례로(컨테이너 기본 명령). 대조가 실패하면 종료 코드 1
"""
import argparse
import json
import sys

from ..config import load_settings
from ..db import connect
from ..object_store import client
from . import export, reconcile


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Gold → Parquet → Spark → 대조")
    parser.add_argument("step", choices=["export", "spark", "reconcile", "all"])
    parser.add_argument("--snapshot", help="처리할 스냅샷(기본: 가장 최근 내보내기)")
    args = parser.parse_args(argv)
    settings = load_settings()
    store, bucket = client(settings), settings.lake_bucket
    report, snapshot = {}, args.snapshot
    if args.step in ("export", "all"):
        with connect(settings) as conn:
            manifest = export.export(conn, store, bucket)
        snapshot = manifest["snapshot"]
        report["export"] = {"snapshot": snapshot, "uploaded_files": manifest["uploaded_files"],
                            "rows": {name: table["rows"] for name, table in manifest["tables"].items()}}
    if args.step in ("spark", "all"):
        from . import spark_features   # pyspark가 필요한 단계에서만 불러온다
        report["spark"] = spark_features.run(store, bucket, snapshot)
        snapshot = report["spark"]["snapshot"]
    if args.step in ("reconcile", "all"):
        report["reconcile"] = reconcile.reconcile(store, bucket, snapshot)
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0 if report.get("reconcile", {}).get("passed", True) else 1


if __name__ == "__main__":
    sys.exit(main())
