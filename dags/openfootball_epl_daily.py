"""매일 EPL 일정·결과를 수집하고 Gold 테이블을 다시 만드는 DAG(로드맵 1d).

순서: 스키마 확인 → 수집할 시즌 결정 → 시즌별 수집(동적 태스크) → dbt build(모델 + 품질 테스트)
- 수집은 멱등이라 같은 날 여러 번 실행해도 원본·경기 버전이 늘지 않는다. Gold는 매번 테이블을 다시 만든다.
- 아직 openfootball에 올라오지 않은 시즌(404)은 실패가 아니라 건너뜀으로 처리한다.
- dbt 품질 테스트가 하나라도 실패하면 DAG 실행이 실패로 표시된다.
"""
import urllib.error
from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from pipelines.schedule import seasons_for

LEAGUE = "en.1"


@dag(
    dag_id="openfootball_epl_daily",
    schedule="0 6 * * *",  # 매일 06:00 UTC(한국 15:00). 전날 경기 결과가 반영된 뒤
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,  # 꺼져 있던 동안의 실행을 몰아서 하지 않는다. 수집은 최신 원본 기준이라 한 번이면 충분하다.
    max_active_runs=1,  # 같은 데이터셋을 동시에 수집하지 않게 한 번에 하나만 실행
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5), "retry_exponential_backoff": True},
    tags=["ingest", "openfootball", "dbt"],
    doc_md=__doc__,
)
def openfootball_epl_daily():
    @task
    def migrate_schema():
        """Silver 스키마가 최신인지 확인하고, 새 SQL 파일이 있으면 적용한다."""
        from pipelines.db import connect, migrate
        with connect() as conn:
            return migrate(conn)

    @task
    def seasons_to_ingest():
        """이번 실행에서 수집할 시즌 목록(이전·현재 시즌)."""
        return seasons_for(datetime.now(timezone.utc).date())

    @task
    def ingest_season(season: str):
        """시즌 하나를 수집하고 실행 통계를 돌려준다(XCom으로 Airflow 화면에서 확인 가능)."""
        from pipelines.ingest import openfootball
        try:
            return openfootball.ingest(season, LEAGUE)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise AirflowSkipException(f"{season} 시즌 파일이 아직 없습니다.")
            raise

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/project && python -m pipelines.dbt_runner build",
        trigger_rule="none_failed",  # 일부 시즌이 건너뜀이어도 Gold는 만든다. 실패가 있으면 만들지 않는다.
    )

    migrate_schema() >> ingest_season.expand(season=seasons_to_ingest()) >> dbt_build


openfootball_epl_daily()
