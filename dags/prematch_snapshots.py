"""경기 시작 24·3·1시간 전 입력 스냅샷 DAG(로드맵 2e).

30분마다 실행한다. 스냅샷 시점에서 90분 안이면 찍고(TAKEN), 더 늦었으면 MISSED로 남긴다.
PC나 Airflow가 꺼져 있던 동안의 시점은 나중에 채우지 않는다. 그래야 저장된 스냅샷이 "그때 알 수 있던 입력"이 된다.
"""
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task


@dag(
    dag_id="prematch_snapshots",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["snapshots", "prospective"],
    doc_md=__doc__,
)
def prematch_snapshots():
    @task
    def take_due_snapshots():
        from pipelines import snapshots
        return snapshots.run()

    take_due_snapshots()


prematch_snapshots()
