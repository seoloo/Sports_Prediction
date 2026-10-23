"""dbt 실행 도우미.

dbt는 다른 패키지(Airflow 등)와 의존성이 충돌하지 않게 별도 가상환경(.venv-dbt, 컨테이너에서는 /opt/dbt-venv)에 둔다.
이 모듈은 .env의 Postgres 접속 정보를 dbt 환경 변수(DBT_POSTGRES_*)로 넘겨 dbt를 실행한다.
실행: python -m pipelines.dbt_runner build   (그 밖의 dbt 인자도 그대로 넘길 수 있다)
"""
import os
import subprocess
import sys
from pathlib import Path

from .config import PROJECT_ROOT, Settings, load_settings

DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"


def find_dbt_executable() -> str:
    """dbt 실행 파일을 찾는다. DBT_EXECUTABLE 환경 변수 → 프로젝트 .venv-dbt 순서."""
    if os.environ.get("DBT_EXECUTABLE"):
        return os.environ["DBT_EXECUTABLE"]
    for candidate in (PROJECT_ROOT / ".venv-dbt" / "Scripts" / "dbt.exe", PROJECT_ROOT / ".venv-dbt" / "bin" / "dbt"):
        if candidate.exists():
            return str(candidate)
    raise FileNotFoundError("dbt를 찾지 못했습니다. README의 dbt 설치 단계(.venv-dbt)를 먼저 실행하세요.")


def dbt_environment(settings: Settings) -> dict:
    """현재 환경 변수에 dbt 접속 정보를 더한 dict. 비밀번호는 명령줄 인자가 아니라 환경 변수로만 넘긴다.

    PYTHONUTF8=1: 한국어 Windows는 기본 인코딩이 CP949라 한글 주석이 든 dbt 파일을 읽다 실패하므로 UTF-8을 강제한다.
    """
    return {**os.environ, "PYTHONUTF8": "1",
            "DBT_POSTGRES_HOST": settings.postgres_host, "DBT_POSTGRES_PORT": str(settings.postgres_port),
            "DBT_POSTGRES_USER": settings.postgres_user, "DBT_POSTGRES_PASSWORD": settings.postgres_password,
            "DBT_POSTGRES_DB": settings.postgres_db}


def run_dbt(arguments: list[str], settings: Settings | None = None) -> int:
    """dbt를 실행하고 종료 코드를 돌려준다. 0이 아니면 모델 실패나 품질 테스트 실패다."""
    command = [find_dbt_executable(), *arguments,
               "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)]
    return subprocess.run(command, env=dbt_environment(settings or load_settings()), cwd=DBT_PROJECT_DIR).returncode


if __name__ == "__main__":
    sys.exit(run_dbt(sys.argv[1:] or ["build"]))
