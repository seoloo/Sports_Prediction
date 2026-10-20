"""스케줄 관련 순수 함수. Airflow 없이 단위 테스트할 수 있도록 DAG 파일에서 분리했다."""
from datetime import date


def season_label(start_year: int) -> str:
    """시즌 시작 연도 → openfootball 시즌 폴더 이름. 예: 2025 → "2025-26", 2099 → "2099-00"."""
    return f"{start_year}-{(start_year + 1) % 100:02d}"


def seasons_for(today: date) -> list[str]:
    """오늘 기준 (이전 시즌, 현재 시즌). 유럽 리그는 7월에 새 시즌이 시작한다고 본다.

    예: 2026-10-21 → ["2025-26", "2026-27"], 2026-05-01 → ["2024-25", "2025-26"]
    이전 시즌도 함께 수집하는 이유: 시즌이 끝난 뒤에도 결과 정정이 올라올 수 있기 때문이다.
    """
    start_year = today.year if today.month >= 7 else today.year - 1
    return [season_label(start_year - 1), season_label(start_year)]
