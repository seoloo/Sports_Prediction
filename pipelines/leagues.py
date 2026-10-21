"""다루는 축구 리그 목록(로드맵 5a). 소스마다 리그를 부르는 이름이 달라 한곳에서 대응시킨다.

- code: openfootball 리그 코드이자 silver.matches.competition_code
- fdorg: football-data.org 대회 코드(무료 등급에서 확인: PL, BL1, PD, SA, DED)
- odds_api: The Odds API 리그 키(5b에서 사용)
- couk: football-data.co.uk 리그 코드(과거 배당 CSV 파일 이름)
MODELED: 모델을 학습·평가한 리그. 나머지 리그의 경기 전 예측은 확률 없이 LEAGUE_NOT_MODELED로 보류한다.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class League:
    code: str
    name: str
    fdorg: str
    odds_api: str
    couk: str
    timezone: str


LEAGUES = (
    League("en.1", "Premier League", "PL", "soccer_epl", "E0", "Europe/London"),
    League("de.1", "Bundesliga", "BL1", "soccer_germany_bundesliga", "D1", "Europe/Berlin"),
    League("es.1", "La Liga", "PD", "soccer_spain_la_liga", "SP1", "Europe/Madrid"),
    League("it.1", "Serie A", "SA", "soccer_italy_serie_a", "I1", "Europe/Rome"),
    League("nl.1", "Eredivisie", "DED", "soccer_netherlands_eredivisie", "N1", "Europe/Amsterdam"),
)
BY_CODE = {league.code: league for league in LEAGUES}
BY_FDORG = {league.fdorg: league for league in LEAGUES}
MODELED = ("en.1",)
