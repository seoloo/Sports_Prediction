"""Spark로 시간순 특징을 다시 계산한다(로드맵 4a). 규칙은 dbt gold.match_features_pit와 같다.

경기 M의 한 팀 특징 = 그 팀의 끝난 경기 중 "시작 + 3시간 <= M의 시작"이고 M이 아닌 경기의 최근 10·38경기 평균.
dbt는 경기마다 LATERAL 하위 쿼리로 최근 N경기를 고르고, 여기서는 같은 조건의 조인 + row_number 창 함수로 고른다.
같은 규칙을 다른 엔진·다른 방식으로 구현해 결과를 대조하는 것이 이 단계의 목적이다(pipelines/lake/reconcile.py).

7b부터 입력과 결과 모두 Iceberg 테이블이다. 입력은 gold.matches 등을 "그 내보내기를 쓴 스냅샷"으로 읽고(시간 여행),
결과는 spark.match_features_pit, spark.nba_standings에 내보내기 ID를 단 스냅샷으로 쓴다. 카탈로그는 PyIceberg와 같은
Postgres(Iceberg JDBC 카탈로그), 파일 입출력은 Iceberg S3FileIO(AWS SDK 번들)라 hadoop-aws(s3a)가 필요 없다.
jar는 이미지에 고정해 둔 것만 쓴다(ICEBERG_JARS, infra/spark/fetch_jars.py).
Spark는 이 모듈 안에서만 불러온다(호스트의 다른 코드와 테스트는 pyspark 없이 돈다).
실행(컨테이너): python -m pipelines.lake.run spark
"""
import json
import os
from pathlib import Path

from ..object_store import read_object
from .export import PREFIX

AVAILABILITY_LAG_HOURS = 3   # 과거 재구성 가정: 결과는 시작 3시간 뒤에 알려짐(dbt 모델·dataset.py와 같은 값)
FEATURES_TABLE, NBA_TABLE = "match_features_pit", "nba_standings"   # spark.* 결과 테이블(7b)


def iceberg_conf(settings, jar_dir: str) -> dict:
    """Spark가 PyIceberg와 같은 카탈로그·저장소를 보게 하는 설정(순수 함수, 테스트 가능)."""
    from .iceberg import CATALOG_NAME, WAREHOUSE_PREFIX
    prefix = f"spark.sql.catalog.{CATALOG_NAME}"
    jars = sorted(str(path) for path in Path(jar_dir).glob("*.jar"))
    return {"spark.jars": ",".join(jars),
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            prefix: "org.apache.iceberg.spark.SparkCatalog",
            f"{prefix}.type": "jdbc",
            f"{prefix}.uri": (f"jdbc:postgresql://{settings.postgres_host}:{settings.postgres_port}/"
                              f"{settings.postgres_db}?currentSchema=lakehouse"),
            f"{prefix}.jdbc.user": settings.postgres_user,
            f"{prefix}.jdbc.password": settings.postgres_password,
            f"{prefix}.warehouse": f"s3://{settings.lake_bucket}/{WAREHOUSE_PREFIX}",
            f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            f"{prefix}.s3.endpoint": settings.s3_endpoint_url,
            f"{prefix}.s3.path-style-access": "true",
            f"{prefix}.s3.access-key-id": settings.s3_access_key,
            f"{prefix}.s3.secret-access-key": settings.s3_secret_key,
            f"{prefix}.client.region": "us-east-1"}


def spark_session(app_name: str = "match_features_pit", settings=None):
    """단일 컨테이너 local 모드 세션. 시간대를 UTC로 고정해 시각 계산이 실행 환경에 따라 달라지지 않게 한다.
    settings와 ICEBERG_JARS가 있으면 Iceberg 카탈로그를 붙인다."""
    from pyspark.sql import SparkSession
    builder = SparkSession.builder.master("local[2]").appName(app_name)
    jar_dir = os.environ.get("ICEBERG_JARS")
    if settings is not None and jar_dir:
        for key, value in iceberg_conf(settings, jar_dir).items():
            builder = builder.config(key, value)
    return (builder
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


def compute_nba_standings(games):
    """끝난 NBA 경기(game_id, season, postseason, home_team, away_team, home_score, away_score)로 정규 시즌 순위표(6b).

    규칙은 dbt gold.nba_standings와 같다: 정규 시즌만, 승률 → 득실 차 → 팀 이름. dbt는 팀별로 펼친 Gold를 모으고,
    여기서는 원자료를 직접 펼쳐 창 함수로 순위를 매긴다. 다른 엔진·다른 경로로 같은 규칙을 구현해 대조한다.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    regular = games.where(~F.col("postseason"))
    sides = (regular.select("season", F.col("home_team").alias("team"), F.col("home_score").alias("points_for"),
                            F.col("away_score").alias("points_against"))
             .unionByName(regular.select("season", F.col("away_team").alias("team"), F.col("away_score").alias("points_for"),
                                         F.col("home_score").alias("points_against"))))
    totals = sides.groupBy("season", "team").agg(
        F.count(F.lit(1)).alias("played"),
        F.count(F.when(F.col("points_for") > F.col("points_against"), True)).alias("won"),
        F.count(F.when(F.col("points_for") < F.col("points_against"), True)).alias("lost"),
        F.sum("points_for").cast("long").alias("points_for"),
        F.sum("points_against").cast("long").alias("points_against"))
    totals = (totals.withColumn("point_difference", F.col("points_for") - F.col("points_against"))
              .withColumn("ratio", F.col("won") / F.col("played")))
    order = Window.partitionBy("season").orderBy(F.col("ratio").desc(), F.col("point_difference").desc(), F.col("team"))
    return (totals.withColumn("position", F.row_number().over(order))
            .withColumn("win_pct", F.col("ratio"))
            .withColumn("avg_margin", F.col("point_difference") / F.col("played"))
            .drop("ratio").orderBy("season", "position"))


def write_result(spark, frame, name: str, export: str) -> None:
    """결과를 spark.<name> Iceberg 테이블의 새 스냅샷으로(전체 덮어쓰기, 요약에 내보내기 ID). 없으면 시즌 파티션으로 만든다."""
    from pyspark.sql import functions as F
    from .iceberg import CATALOG_NAME, SNAPSHOT_PROPERTY, SPARK_NAMESPACE
    table = f"{CATALOG_NAME}.{SPARK_NAMESPACE}.{name}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{SPARK_NAMESPACE}")
    if not spark.catalog.tableExists(table):
        frame.limit(0).writeTo(table).using("iceberg").partitionedBy(F.col("season")).create()
    frame.writeTo(table).option(f"snapshot-property.{SNAPSHOT_PROPERTY}", export).overwrite(F.lit(True))


def run(settings, store, bucket: str, snapshot: str | None = None) -> dict:
    """내보내기 스냅샷의 Iceberg 입력으로 특징·순위표를 계산해 spark.* Iceberg 테이블에 쓴다.
    이미 그 내보내기의 결과가 있으면 다시 계산하지 않는다(멱등)."""
    from . import iceberg
    snapshot = snapshot or json.loads(read_object(store, bucket, f"{PREFIX}/latest.json"))["snapshot"]
    cat = iceberg.catalog(settings, bucket)
    done = {}
    for name in (FEATURES_TABLE, NBA_TABLE):
        identifier = f"{iceberg.SPARK_NAMESPACE}.{name}"
        if cat.table_exists(identifier):
            found = [item for item in iceberg.history(cat.load_table(identifier)) if item["export"] == snapshot]
            if found:
                done[name] = found[-1]["rows"]
    if len(done) == 2:
        return {"snapshot": snapshot, "rows": done[FEATURES_TABLE], "nba_standings_rows": done[NBA_TABLE],
                "newly_computed": False}
    inputs = {name: iceberg.snapshot_for_export(cat.load_table(f"{iceberg.NAMESPACE}.{name}"), snapshot)
              for name in ("matches", "team_match_results", "nba_games")}
    spark = spark_session(settings=settings)
    # 시간 여행: 그 내보내기를 쓴 스냅샷. Iceberg 1.12부터 읽기 옵션(snapshot-id) 대신 SQL VERSION AS OF를 쓴다.
    at_export = lambda name: spark.sql(
        f"SELECT * FROM {iceberg.CATALOG_NAME}.{iceberg.NAMESPACE}.{name} VERSION AS OF {int(inputs[name])}")
    features = compute_features(at_export("matches"), at_export("team_match_results"))
    standings = compute_nba_standings(at_export("nba_games"))
    write_result(spark, features, FEATURES_TABLE, snapshot)
    write_result(spark, standings, NBA_TABLE, snapshot)
    report = {"snapshot": snapshot, "rows": features.count(), "nba_standings_rows": standings.count(),
              "input_snapshots": inputs, "spark_version": spark.version,
              "rule": f"result known at kickoff + {AVAILABILITY_LAG_HOURS}h", "newly_computed": True}
    spark.stop()
    return report
