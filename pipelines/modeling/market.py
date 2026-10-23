"""해외 시장 기준 확률(모델 평가의 기준선).

북메이커 배당에서 마진을 뺀 확률을 "시장이 매긴 확률"로 보고, 모델 예측을 채점할 때 같은 경기·같은 시점의 비교 대상으로 쓴다.
입력은 경기 전 스냅샷의 odds 목록이라 그 시점에 알려진 배당만 들어 있다(시점 누수 없음).
- 축구 1X2: football-data.co.uk 평균(AVG, 직접 넣은 경우) → The Odds API의 Pinnacle → 북메이커 3곳 이상의 평균.
- NBA 승패(H2H): Pinnacle → 북메이커 3곳 이상의 평균.
어느 출처를 썼는지 함께 돌려준다.
"""
from datetime import datetime

import psycopg

FOOTBALL_OUTCOMES = ("HOME", "DRAW", "AWAY")
NBA_OUTCOMES = ("HOME", "AWAY")
MIN_CONSENSUS_BOOKMAKERS = 3


def latest_prices(odds: list[dict], market: str, outcomes: tuple, source_id: str, bookmaker: str | None = None) -> dict | None:
    """odds 목록에서 해당 소스·시장의 마감 전 배당 {선택지: 배당}. 선택지가 모두 있어야 한다."""
    prices = {row["selection"]: float(row["price"]) for row in odds
              if row["source_id"] == source_id and row["market"] == market and row.get("timing", "PRE_MATCH") == "PRE_MATCH"
              and (bookmaker is None or row["bookmaker"] == bookmaker)}
    return prices if set(prices) == set(outcomes) else None


def fair_from_prices(prices: dict) -> dict:
    """배당 → 마진을 뺀 확률(역수를 합이 1이 되게 나눈다)."""
    inverse = {outcome: 1 / price for outcome, price in prices.items()}
    return {outcome: value / sum(inverse.values()) for outcome, value in inverse.items()}


def _market_fair(odds: list[dict], market: str, outcomes: tuple, sources: list[tuple]) -> tuple[dict | None, str | None]:
    for source_id, bookmaker in sources:
        prices = latest_prices(odds, market, outcomes, source_id, bookmaker)
        if prices:
            return fair_from_prices(prices), f"{source_id}:{bookmaker}"
    books = sorted({row["bookmaker"] for row in odds if row["source_id"] == "the-odds-api"})
    fairs = [fair_from_prices(p) for p in (latest_prices(odds, market, outcomes, "the-odds-api", book) for book in books) if p]
    if len(fairs) >= MIN_CONSENSUS_BOOKMAKERS:
        return {o: sum(f[o] for f in fairs) / len(fairs) for o in outcomes}, f"the-odds-api:consensus({len(fairs)})"
    return None, None


def football_market_fair(odds: list[dict]) -> tuple[dict | None, str | None]:
    """축구 1X2 시장 기준 확률과 출처. 우선순위: co.uk 평균 → Pinnacle → 북메이커 평균(3곳 이상)."""
    return _market_fair(odds, "1X2", FOOTBALL_OUTCOMES,
                        [("football-data-co-uk", "AVG"), ("the-odds-api", "pinnacle")])


def nba_market_fair(odds: list[dict]) -> tuple[dict | None, str | None]:
    """NBA 승패 시장 기준 확률과 출처. Pinnacle → 북메이커 평균(3곳 이상)."""
    return _market_fair(odds, "H2H", NBA_OUTCOMES, [("the-odds-api", "pinnacle")])


def nba_odds_at(conn: psycopg.Connection, game_id: int, as_of: datetime) -> list[dict]:
    """as_of에 알려진 NBA 경기 배당(모든 시장·라인)."""
    return [dict(zip(("source_id", "bookmaker", "market", "selection", "price", "line"), values)) for values in conn.execute(
        """SELECT source_id, bookmaker, market, selection, price, line FROM silver.game_odds_known_at(%s)
           WHERE game_id = %s ORDER BY source_id, bookmaker, market, line, selection""", (as_of, game_id)).fetchall()]
