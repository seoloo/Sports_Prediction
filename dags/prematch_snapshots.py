"""경기 시작 24·3·1시간 전 입력 스냅샷 DAG(로드맵 2e).

30분마다 실행한다. 스냅샷 시점에서 90분 안이면 찍고(TAKEN), 더 늦었으면 MISSED로 남긴다.
PC나 Airflow가 꺼져 있던 동안의 시점은 나중에 채우지 않는다. 그래야 저장된 스냅샷이 "그때 알 수 있던 입력"이 된다.
새로 찍거나 놓친 스냅샷은 텔레그램으로 알린다(설정했을 때만). 알림 실패는 스냅샷과 별개의 작업이라 스냅샷을 잃지 않는다.
5b: 스냅샷 직전에 The Odds API로 곧 스냅샷이 올 리그의 배당을 받는다. 배당 수집이 실패해도 스냅샷은 찍는다
(그때 알려진 배당으로).
5d-b: NBA 배당도 같은 주기로 본다(그날 첫 경기 1시간 전에만 받음). 축구와 별개 작업이라 한쪽 실패가 다른 쪽을 막지 않는다.
"""
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

from pipelines.notify.alerts import airflow_failure_callback


@dag(
    dag_id="prematch_snapshots",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5), "on_failure_callback": airflow_failure_callback},
    tags=["snapshots", "prospective", "notify"],
    doc_md=__doc__,
)
def prematch_snapshots():
    @task(retries=0)   # 실패해도 다음 30분에 다시 본다. 크레딧을 재시도로 쓰지 않는다
    def fetch_due_odds():
        from pipelines.ingest import the_odds_api
        return the_odds_api.run_due()

    @task(retries=0)
    def fetch_due_nba_odds():
        from pipelines.ingest import the_odds_api_nba
        return the_odds_api_nba.run_due()

    @task(trigger_rule="all_done")   # 배당 수집이 실패해도 스냅샷은 찍는다
    def take_due_snapshots():
        from pipelines import snapshots
        return snapshots.run()

    @task
    def notify_snapshots():
        from pipelines.notify import alerts
        return alerts.notify_prematch()

    [fetch_due_odds(), fetch_due_nba_odds()] >> take_due_snapshots() >> notify_snapshots()


prematch_snapshots()
