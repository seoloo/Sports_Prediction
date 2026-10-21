"""Spark로 시간순 특징을 다시 계산한다(로드맵 4a). 규칙은 dbt gold.match_features_pit와 같다.

경기 M의 한 팀 특징 = 그 팀의 끝난 경기 중 "시작 + 3시간 <= M의 시작"이고 M이 아닌 경기의 최근 10·38경기 평균.
dbt는 경기마다 LATERAL 하위 쿼리로 최근 N경기를 고르고, 여기서는 같은 조건의 조인 + row_number 창 함수로 고른다.
같은 규칙을 다른 엔진·다른 방식으로 구현해 결과를 대조하는 것이 이 단계의 목적이다(pipelines/lake/reconcile.py).

S3는 boto3로 내려받고 올린다. Spark의 S3 커넥터(s3a)는 hadoop-aws와 수백 MB의 AWS SDK 묶음이 필요한데,
지금 자료는 수 MB라 과하다. 자료가 커지면 s3a로 바로 읽고 쓰는 방식으로 바꾼다.
Spark는 이 모듈 안에서만 불러온다(호스트의 다른 코드와 테스트는 pyspark 없이 돈다).
실행(컨테이너): python -m pipelines.lake.run spark
"""
import json
import tempfile
from pathlib import Path

from ..object_store import put_if_absent, read_object
from .export import PARQUET, PREFIX

AVAILABILITY_LAG_HOURS = 3   # 과거 재구성 가정: 결과는 시작 3시간 뒤에 알려짐(dbt 모델·dataset.py와 같은 값)
OUTPUT_PREFIX = "spark/match_features_pit"


def spark_session(app_name: str = "match_features_pit"):
    """단일 컨테이너 local 모드 세션. 시간대를 UTC로 고정해 시각 계산이 실행 환경에 따라 달라지지 않게 한다."""
    from pyspark.sql import SparkSession
    return (SparkSession.builder.master("local[2]").appName(app_name)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "4")   # 자료가 작아 기본 200개 분할은 낭비
            # 시각을 표준 Parquet 형식(UTC 마이크로초)으로 쓴다. 기본 INT96은 다른 도구가 시간대 없이 읽는다.
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
            # season=2025-26 같은 경로 값을 날짜 등으로 추측하지 않고 문자열로 둔다.
            .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")   # 진행 막대를 끄면 Airflow 로그가 깨끗하다
            .getOrCreate())


def compute_features(matches, history):
    """경기(match_id, kickoff_utc, home_team, away_team, season)와 팀별 끝난 경기로 양 팀 특징을 계산한다."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    sides = (matches.select("match_id", "season", "kickoff_utc", F.col("home_team").alias("team"), F.lit("home").alias("side"))
             .unionByName(matches.select("match_id", "season", "kickoff_utc", F.col("away_team").alias("team"),
                                         F.lit("away").alias("side"))))
    past = history.select(F.col("team").alias("past_team"), F.col("match_id").alias("past_match_id"),
                          F.col("kickoff_utc").alias("past_kickoff"), "goals_for", "goals_against", "points")
    known = (sides.join(past, (sides.team == past.past_team) & (past.past_match_id != sides.match_id)
                        & (past.past_kickoff + F.expr(f"INTERVAL {AVAILABILITY_LAG_HOURS} HOURS") <= sides.kickoff_utc),
                        "left")
             .withColumn("recency", F.row_number().over(
                 Window.partitionBy("match_id", "side").orderBy(F.col("past_kickoff").desc_nulls_last(),
                                                                F.col("past_match_id").desc()))))
    last10, last38 = F.col("recency") <= 10, F.col("recency") <= 38
    per_side = known.groupBy("match_id", "season", "kickoff_utc", "side").agg(
        F.count(F.when(last10, F.col("past_match_id"))).alias("l10_n"),
        F.avg(F.when(last10, F.col("goals_for"))).alias("l10_gf"),
        F.avg(F.when(last10, F.col("goals_against"))).alias("l10_ga"),
        F.avg(F.when(last10, F.col("points"))).alias("l10_pts"),
        F.count(F.when(last38, F.col("past_match_id"))).alias("l38_n"),
        F.avg(F.when(last38, F.col("goals_for"))).alias("l38_gf"),
        F.avg(F.when(last38, F.col("goals_against"))).alias("l38_ga"),
        F.max(F.when(last10, F.col("past_kickoff"))).alias("last_feature_kickoff"))
    per_side = per_side.withColumn(
        "rest_days", (F.unix_timestamp("kickoff_utc") - F.unix_timestamp("last_feature_kickoff")) / 86400.0)
    columns = ["l10_n", "l10_gf", "l10_ga", "l10_pts", "l38_n", "l38_gf", "l38_ga", "rest_days", "last_feature_kickoff"]
    home = per_side.where("side = 'home'").select("match_id", "season", "kickoff_utc",
                                                  *[F.col(c).alias(f"home_{c}") for c in columns])
    away = per_side.where("side = 'away'").select("match_id", *[F.col(c).alias(f"away_{c}") for c in columns])
    return home.join(away, "match_id").orderBy("kickoff_utc", "match_id")


def download_prefix(store, bucket: str, prefix: str, target: Path) -> int:
    """S3의 prefix 아래 객체를 같은 상대 경로로 내려받는다. 받은 파일 수를 돌려준다."""
    count = 0
    for page in store.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            path = target / item["Key"][len(prefix):].lstrip("/")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(read_object(store, bucket, item["Key"]))
            count += 1
    return count


def run(store, bucket: str, snapshot: str | None = None) -> dict:
    """스냅샷 입력으로 특징을 계산해 lake 버킷에 올린다. 이미 결과가 있으면 다시 계산하지 않는다(멱등)."""
    snapshot = snapshot or json.loads(read_object(store, bucket, f"{PREFIX}/latest.json"))["snapshot"]
    output = f"{OUTPUT_PREFIX}/snapshot={snapshot}"
    existing = store.list_objects_v2(Bucket=bucket, Prefix=f"{output}/_manifest.json").get("KeyCount", 0)
    if existing:
        return {**json.loads(read_object(store, bucket, f"{output}/_manifest.json")), "newly_computed": False}
    spark = spark_session()
    with tempfile.TemporaryDirectory() as work:
        work = Path(work)
        download_prefix(store, bucket, f"{PREFIX}/{snapshot}/", work / "input")
        matches = spark.read.parquet(str(work / "input" / "matches"))
        history = spark.read.parquet(str(work / "input" / "team_match_results"))
        features = compute_features(matches, history)
        features.coalesce(1).write.parquet(str(work / "output"))   # 자료가 작아 파일 하나로
        (part,) = sorted((work / "output").glob("part-*.parquet"))
        rows = features.count()
        put_if_absent(store, bucket, f"{output}/part-0.parquet", part.read_bytes(), {"rows": str(rows)},
                      content_type=PARQUET)
        manifest = {"snapshot": snapshot, "rows": rows, "spark_version": spark.version,
                    "rule": f"result known at kickoff + {AVAILABILITY_LAG_HOURS}h", "key": f"{output}/part-0.parquet"}
        put_if_absent(store, bucket, f"{output}/_manifest.json", json.dumps(manifest).encode(), {"snapshot": snapshot})
    spark.stop()
    return {**manifest, "newly_computed": True}
