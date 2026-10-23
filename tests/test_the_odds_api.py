"""5b The Odds API 수집기 테스트. 실제로 호출하지 않는다(가짜 응답). 연결·저장은 Postgres가 있을 때만."""
import io
import json
import urllib.error
from datetime import datetime, timedelta, timezone

import pytest

from pipelines.ingest import openfootball
from pipelines.ingest import the_odds_api as odds_api
from pipelines.modeling.market import football_market_fair as market_fair
from tests.test_pipeline_integration import SAMPLE_PATH, isolated_settings, query  # noqa: F401 (fixture 재사용)

KICKOFF = datetime(2026, 5, 24, 15, 0, tzinfo=timezone.utc)   # 예시 원본 Delta FC - Alpha FC
KEY = "secret-odds-key-0123456789abcdef"


def event(home="Delta", away="Alpha", kickoff=KICKOFF, books=None):
    books = books if books is not None else [
        {"key": "pinnacle", "last_update": "2026-05-23T10:00:00Z",
         "markets": [{"key": "h2h", "last_update": "2026-05-23T10:00:00Z",
                      "outcomes": [{"name": home, "price": 2.0}, {"name": away, "price": 4.0}, {"name": "Draw", "price": 4.0}]}]},
        {"key": "betfair_ex_eu", "last_update": "2026-05-23T11:00:00Z",
         "markets": [{"key": "h2h", "last_update": "2026-05-23T11:00:00Z",
                      "outcomes": [{"name": home, "price": 2.1}, {"name": away, "price": 3.9}, {"name": "Draw", "price": 1.0}]}]}]
    return {"id": "e1", "commence_time": kickoff.strftime("%Y-%m-%dT%H:%M:%SZ"), "home_team": home, "away_team": away,
            "bookmakers": books}


def test_due_only_just_before_24h_and_1h_snapshots():
    events = [event()]
    assert odds_api.due_offsets(events, KICKOFF - timedelta(hours=24, minutes=20)) == ["T-24H 2026-05-24T15:00"]
    assert odds_api.due_offsets(events, KICKOFF - timedelta(hours=1, minutes=30)) == ["T-1H 2026-05-24T15:00"]
    assert odds_api.due_offsets(events, KICKOFF - timedelta(hours=3, minutes=10)) == []        # 3시간 전은 받지 않음
    assert odds_api.due_offsets(events, KICKOFF - timedelta(hours=24, minutes=20), low_credits=True) == []   # 크레딧 부족


def test_quotes_map_names_and_drop_malformed_books():
    rows = odds_api.quotes(event())
    assert {(r["bookmaker"], r["selection"], r["price"]) for r in rows} == {
        ("pinnacle", "HOME", 2.0), ("pinnacle", "DRAW", 4.0), ("pinnacle", "AWAY", 4.0)}   # 1.0 배당이 있는 곳은 버림
    assert rows[0]["published_at"] == datetime(2026, 5, 23, 10, tzinfo=timezone.utc)


def test_similarity_handles_short_and_official_names():
    assert odds_api.similarity("Bayern Munich", "FC Bayern München") > odds_api.similarity("Bayern Munich", "Borussia Dortmund")
    assert odds_api.similarity("Inter Milan", "FC Internazionale Milano") > odds_api.similarity("Inter Milan", "AC Milan") - 0.2


def test_errors_never_leak_the_key():
    def failing(url, timeout):
        raise urllib.error.URLError(f"cannot reach {url}")
    with pytest.raises(odds_api.OddsApiError) as caught:
        odds_api.call("/sports/soccer_epl/odds", KEY, opener=failing)
    assert KEY not in str(caught.value) and caught.value.__cause__ is None


def test_market_fair_prefers_pinnacle_then_consensus():
    def quote(bookmaker, selection, price, source="the-odds-api"):
        return {"source_id": source, "bookmaker": bookmaker, "market": "1X2", "selection": selection,
                "timing": "PRE_MATCH", "price": price}
    pinnacle = [quote("pinnacle", s, p) for s, p in (("HOME", 2.0), ("DRAW", 4.0), ("AWAY", 4.0))]
    others = [quote(book, s, p) for book in ("a", "b", "c") for s, p in (("HOME", 1.8), ("DRAW", 4.0), ("AWAY", 5.0))]
    fair, source = market_fair(pinnacle + others)
    assert source == "the-odds-api:pinnacle" and fair["HOME"] == pytest.approx(0.5)
    fair, source = market_fair(others)
    assert source == "the-odds-api:consensus(3)" and sum(fair.values()) == pytest.approx(1)
    assert market_fair(others[:6]) == (None, None)   # 북메이커 2곳뿐이면 기준으로 삼지 않음


class FakeResponse(io.BytesIO):
    def __init__(self, body, remaining="400"):
        super().__init__(json.dumps(body).encode())
        self.headers = {"x-requests-remaining": remaining, "x-requests-last": "1"}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@pytest.fixture
def season(isolated_settings, tmp_path, monkeypatch):
    openfootball.ingest("2025-26", "en.1", payload=SAMPLE_PATH.read_bytes(), source_url="file:sample.json",
                        settings=isolated_settings, received_at=datetime(2025, 8, 17, tzinfo=timezone.utc))
    aliases = tmp_path / "aliases.csv"
    aliases.write_text("source_id,alias,canonical_name,evidence\n" + "".join(
        f"the-odds-api,{name},{name} FC,test\n" for name in ("Alpha", "Beta", "Gamma", "Delta")), encoding="utf-8")
    monkeypatch.setattr(odds_api, "ALIAS_FILE", aliases)
    return isolated_settings.__class__(**{**isolated_settings.__dict__, "the_odds_api_key": KEY})


def test_run_due_fetches_only_due_leagues_and_versions_changes(season):
    calls = []

    def opener(url, timeout):
        calls.append(url)
        if "/soccer_epl/events" in url:
            return FakeResponse([{k: v for k, v in event().items() if k != "bookmakers"}])
        if "/events" in url:
            return FakeResponse([])
        return FakeResponse([event(), event(home="Omega", away="Alpha")])

    first = odds_api.run_due(now=KICKOFF - timedelta(hours=24, minutes=20), settings=season, opener=opener)
    epl = first["leagues"]["en.1"]
    assert epl["fetched"] and (epl["linked"], epl["unmatched"], epl["odds_new"]) == (1, 1, 3)
    assert not first["leagues"]["de.1"]["fetched"]
    assert sum("/odds" in url for url in calls) == 1                     # 크레딧은 EPL 한 번만
    # 90분 안에 다시 불러도 받지 않는다.
    again = odds_api.run_due(now=KICKOFF - timedelta(hours=24, minutes=10), settings=season, opener=opener)
    assert again["leagues"]["en.1"]["fetched"] is False
    rows = query(season, """SELECT bookmaker, selection, price::float, published_at FROM silver.odds_revisions
                            WHERE source_id = 'the-odds-api' ORDER BY selection""")
    assert [r[:3] for r in rows] == [("pinnacle", "AWAY", 4.0), ("pinnacle", "DRAW", 4.0), ("pinnacle", "HOME", 2.0)]
    assert query(season, "SELECT issue_code FROM silver.quality_issues WHERE issue_code = 'UNMAPPED_TEAM'") == [("UNMAPPED_TEAM",)]
    assert KEY not in json.dumps(first, default=str)


def test_run_due_without_key_is_not_configured(isolated_settings):
    without_key = isolated_settings.__class__(**{**isolated_settings.__dict__, "the_odds_api_key": None})
    assert odds_api.run_due(settings=without_key) == {"status": "NOT_CONFIGURED"}   # 실제 API를 부르지 않는다
