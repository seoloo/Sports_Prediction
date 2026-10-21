"""레이크 단계 실행(로드맵 4a): export → spark → reconcile.

python -m pipelines.lake.run export      Gold → Parquet(lake 버킷). 호스트에서도 된다
python -m pipelines.lake.run spark       Spark로 특징 재계산. pyspark가 있는 컨테이너에서만
python -m pipelines.lake.run reconcile   Spark 결과 ↔ dbt 결과 대조. 호스트에서도 된다
python -m pipelines.lake.run all         세 단계를 차례로(컨테이너 기본 명령). 대조가 실패하면 종료 코드 1

Airflow는 run_isolated로 Spark가 설치된 별도 가상환경(SPARK_PYTHON)의 파이썬에서 이 모듈을 실행한다(dbt와 같은 방식).
"""
import argparse
import json
import os
import subprocess
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


def run_isolated(step: str = "all", python: str | None = None) -> dict:
    """Spark가 설치된 파이썬(기본: 환경 변수 SPARK_PYTHON)으로 이 모듈을 실행하고 결과 JSON을 돌려준다.

    Spark 로그는 stderr로, 결과 JSON만 stdout으로 나온다. 대조 실패(종료 코드 1)나 다른 오류면 RuntimeError를 내서
    Airflow 작업이 실패로 표시되게 한다.
    """
    python = python or os.environ.get("SPARK_PYTHON") or sys.executable
    completed = subprocess.run([python, "-m", "pipelines.lake.run", step], capture_output=True, text=True,
                               encoding="utf-8", env={**os.environ, "PYTHONIOENCODING": "utf-8"})
    try:
        report = json.loads(completed.stdout)
    except json.JSONDecodeError:
        report = None
    if completed.returncode != 0 or report is None:
        detail = json.dumps(report.get("reconcile"), ensure_ascii=False) if report else completed.stderr[-2000:]
        raise RuntimeError(f"pipelines.lake.run {step} 실패(종료 코드 {completed.returncode}): {detail}")
    return report


if __name__ == "__main__":
    sys.exit(main())
