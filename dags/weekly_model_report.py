"""경기 전 예측 주간 보고서 DAG(로드맵 3d).

매주 화요일 06:00 UTC(한국 15:00)에 지난주(월~일) 경기를 채점한다. 월요일 밤 경기까지 끝난 뒤다.
- 스냅샷 시점별로 찍음·놓침, 확률 있음, 표시 가능(READY)·보류(WITHHELD)와 보류 사유
- 끝난 경기의 로그 손실·Brier와, 스냅샷에 들어 있던 시장 마감 전 배당과의 비교(이번 주, 누적)
결과는 modeling.prospective_reports에 쌓는다. 결과가 정정되면 다시 실행했을 때 새 버전이 생긴다.
Markdown은 명령줄에서 만든다: python -m pipelines.modeling.prospective report --markdown 경로
보고서 요약은 텔레그램으로도 보낸다(설정했을 때만, 같은 보고서는 한 번만).
"""
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

from pipelines.notify.alerts import airflow_failure_callback


@dag(
    dag_id="weekly_model_report",
    schedule="0 6 * * 2",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10), "on_failure_callback": airflow_failure_callback},
    tags=["modeling", "prospective", "report", "notify"],
    doc_md=__doc__,
)
def weekly_model_report():
    @task
    def score_last_week():
        from pipelines.modeling import prospective
        summary = prospective.run_weekly()
        return {key: summary[key] for key in ("week_start", "report_id", "newly_stored", "snapshots", "withheld_reasons")}

    @task
    def notify_report():
        from pipelines.notify import alerts
        return alerts.notify_weekly()

    score_last_week() >> notify_report()


weekly_model_report()
