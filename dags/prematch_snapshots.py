"""경기 시작 24·3·1시간 전 입력 스냅샷 DAG(로드맵 2e).

30분마다 실행한다. 스냅샷 시점에서 90분 안이면 찍고(TAKEN), 더 늦었으면 MISSED로 남긴다.
PC나 Airflow가 꺼져 있던 동안의 시점은 나중에 채우지 않는다. 그래야 저장된 스냅샷이 "그때 알 수 있던 입력"이 된다.
새로 찍거나 놓친 스냅샷은 텔레그램으로 알린다(설정했을 때만). 알림 실패는 스냅샷과 별개의 작업이라 스냅샷을 잃지 않는다.
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
    @task
    def take_due_snapshots():
        from pipelines import snapshots
        return snapshots.run()

    @task
    def notify_snapshots():
        from pipelines.notify import alerts
        return alerts.notify_prematch()

    take_due_snapshots() >> notify_snapshots()


prematch_snapshots()
