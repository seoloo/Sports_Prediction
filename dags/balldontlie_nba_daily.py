"""NBA 경기 일정·결과 매일 수집 DAG(로드맵 5d).

매일 12:00 UTC(한국 21:00, 미국 동부 08:00)에 현재 시즌을 balldontlie에서 받는다. 전날 밤 미국 경기가 모두 끝난 뒤다.
시즌 하나는 약 14쪽이고 무료 등급 분당 5회를 지키므로 3분쯤 걸린다. 바뀐 경기만 새 버전이 생긴다.
.env에 BALLDONTLIE_API_KEY가 없으면 NOT_CONFIGURED로 끝난다(실패 아님).
축구 DAG와 분리한 이유: 소스·일정·실패 원인이 달라, 한쪽 장애가 다른 쪽 수집을 막지 않게 한다.
"""
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

from pipelines.notify.alerts import airflow_failure_callback


@dag(
    dag_id="balldontlie_nba_daily",
    schedule="0 12 * * *",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=15), "on_failure_callback": airflow_failure_callback},
    tags=["ingest", "nba", "basketball"],
    doc_md=__doc__,
)
def balldontlie_nba_daily():
    @task
    def ingest_current_season():
        from pipelines.ingest import balldontlie
        return balldontlie.ingest(balldontlie.current_season(datetime.now(timezone.utc)))

    ingest_current_season()


balldontlie_nba_daily()
