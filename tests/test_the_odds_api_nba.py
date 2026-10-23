"""5d-b The Odds API NBA 배당 테스트. 실제로 호출하지 않는다(가짜 응답). 저장은 Postgres·S3가 있을 때만."""
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from pipelines.aliases import sync_aliases
from pipelines.db import migrate
from pipelines.ingest import balldontlie as bdl
from pipelines.ingest import the_odds_api_nba as nba
from tests.test_balldontlie import CELTICS, KNICKS, game, page
from tests.test_pipeline_integration import isolated_settings, query  # noqa: F401 (fixture 재사용)
from tests.test_the_odds_api import KEY, FakeResponse

TIP = datetime(2026, 10, 22, 23, 30, tzinfo=timezone.utc)   # 미국 동부 10/22 19:30


def event(home="Boston Celtics", away="New York Knicks", tip=TIP, books=None, event_id="e1"):
    books = books if books is not None else [
        {"key": "pinnacle", "last_update": "2026-10-22T20:00:00Z",
         "markets": [{"key": "h2h", "last_update": "2026-10-22T20:00:00Z",
                      "outcomes": [{"name": home, "price": 1.5}, {"name": away, "price": 2.8}]}]},
        {"key": "broken", "last_update": "2026-10-22T20:00:00Z",
         "markets": [{"key": "h2h", "outcomes": [{"name": home, "price": 1.5}]}]}]
    return {"id": event_id, "commence_time": tip.strftime("%Y-%m-%dT%H:%M:%SZ"), "home_team": home, "away_team": away,
            "bookmakers": books}


def test_one_fetch_per_slate_just_before_the_first_tip():
    late = event(tip=datetime(2026, 10, 23, 2, 30, tzinfo=timezone.utc), event_id="e2")   # 동부 10/22 22:30 → 같은 날
    tomorrow = event(tip=datetime(2026, 10, 23, 23, 0, tzinfo=timezone.utc), event_id="e3")
    events = [event(), late, tomorrow]
    assert nba.slate_first_tips(events) == {TIP.date(): TIP, datetime(2026, 10, 23).date(): datetime(2026, 10, 23, 23, 0, tzinfo=timezone.utc)}
    assert nba.due_reasons(events, TIP - timedelta(hours=1, minutes=20)) == ["FIRST-TIP-1H 2026-10-22T23:30"]
    assert nba.due_reasons(events, TIP - timedelta(hours=3)) == []
    assert nba.due_reasons(events, datetime(2026, 10, 23, 1, 0, tzinfo=timezone.utc)) == []   # 늦은 경기 전에는 다시 받지 않음



def test_quotes_are_two_way_and_drop_incomplete_books():
    rows = nba.quotes(event())
    assert {(r["bookmaker"], r["selection"], r["price"]) for r in rows} == {("pinnacle", "HOME", 1.5), ("pinnacle", "AWAY", 2.8)}
    with_draw = event(books=[{"key": "x", "last_update": "2026-10-22T20:00:00Z", "markets": [{"key": "h2h", "outcomes": [
        {"name": "Boston Celtics", "price": 1.5}, {"name": "New York Knicks", "price": 2.8}, {"name": "Draw", "price": 15.0}]}]}])
    assert nba.quotes(with_draw) == []   # 무승부가 있는 시장은 농구 승패가 아니다


@pytest.fixture
def nba_season(isolated_settings, tmp_path, monkeypatch):
    s = isolated_settings
    scheduled = dict(status="2026-10-22T23:30:00Z", state="scheduled", home_score=0, away_score=0, period=0, day="2026-10-22")
    first = game(1, **scheduled)
    first["datetime"] = "2026-10-22T23:20:00Z"   # 두 소스의 시작 시각이 10분 다르다
    rematch = game(2, **{**scheduled, "day": "2026-12-25"})
    rematch["datetime"] = "2026-12-25T22:00:00Z"   # 같은 대진, 두 달 뒤
    bdl.ingest("2026-27", pages=[page([first, rematch])], settings=s, received_at=datetime(2026, 10, 20, tzinfo=timezone.utc))   # 실제 현재보다 과거여야 시점 조회에 잡힌다
    aliases = tmp_path / "aliases.csv"
    aliases.write_text("source_id,alias,canonical_name,evidence,sport\n"
                       "the-odds-api,Boston Celtics,Boston Celtics,test,basketball\n"
                       "the-odds-api,New York Knicks,New York Knicks,test,basketball\n", encoding="utf-8")
    monkeypatch.setattr(nba, "ALIAS_FILE", aliases)
    return s.__class__(**{**s.__dict__, "the_odds_api_key": KEY})


def test_run_due_links_by_teams_and_nearby_tipoff_and_versions_changes(nba_season):
    s = nba_season
    calls = []
    prices = {"home": 1.5}

    def opener(url, timeout):
        calls.append(url)
        body = [event(), event(home="Boston Celtics", away="Miami Heat", event_id="e9")]
        if "/events" in url:
            return FakeResponse([{k: v for k, v in e.items() if k != "bookmakers"} for e in body])
        market = body[0]["bookmakers"][0]["markets"][0]
        market["outcomes"][0]["price"] = prices["home"]
        if prices["home"] != 1.5:
            market["last_update"] = "2026-10-22T22:50:00Z"   # 북메이커가 배당을 바꾼 시각
        return FakeResponse(body)

    first = nba.run_due(now=TIP - timedelta(hours=1, minutes=20), settings=s, opener=opener)
    assert first["fetched"] and (first["linked"], first["unmatched"], first["odds_new"]) == (1, 1, 2)
    assert sum("/odds" in url for url in calls) == 1
    linked = query(s, """SELECT g.external_id, o.selection, o.price::float FROM silver.game_odds_revisions o
                         JOIN silver.games g USING (game_id) ORDER BY 2""")
    assert linked == [("1", "AWAY", 2.8), ("1", "HOME", 1.5)]   # 12월 재대결이 아니라 10분 차이의 그 경기
    assert query(s, "SELECT issue_code FROM silver.quality_issues WHERE detail->>'away' = 'Miami Heat'") == [("UNMAPPED_TEAM",)]

    assert nba.run_due(now=TIP - timedelta(hours=1), settings=s, opener=opener)["fetched"] is False   # 90분 안 재조회 없음
    prices["home"] = 1.45
    forced = nba.run_due(now=TIP - timedelta(minutes=30), settings=s, force=True, opener=opener)
    assert forced["odds_new"] == 1                                             # 바뀐 HOME만 새 버전
    # 버전 기록: 값과 북메이커가 바꾼 시각(published_at)이 순서대로 남는다. 수신 시각은 실제 시계라 비교하지 않는다
    # (고정 시각으로 시점 조회를 하면 실행 날짜에 따라 결과가 달라진다, 2026-10-23 실패).
    history = query(s, """SELECT revision_no, price::float, published_at FROM silver.game_odds_revisions
                          WHERE selection = 'HOME' ORDER BY revision_no""")
    assert [(r[0], r[1]) for r in history] == [(1, 1.5), (2, 1.45)]
    assert history[0][2] == datetime(2026, 10, 22, 20, tzinfo=timezone.utc)
    assert history[1][2] == datetime(2026, 10, 22, 22, 50, tzinfo=timezone.utc)
    latest = "SELECT price::float FROM silver.game_odds_known_at(now()) WHERE selection = 'HOME'"
    assert query(s, latest) == [(1.45,)]


def test_low_credits_skip_nba(nba_season):
    def opener(url, timeout):
        return FakeResponse([{k: v for k, v in event().items() if k != "bookmakers"}], remaining="40")
    result = nba.run_due(now=TIP - timedelta(hours=1, minutes=20), settings=nba_season, opener=opener)
    assert (result["fetched"], result["reasons"]) == (False, ["LOW_CREDITS"])


def test_basketball_aliases_never_create_football_teams(isolated_settings):
    s = isolated_settings
    now = datetime(2026, 10, 22, tzinfo=timezone.utc)
    with psycopg.connect(s.postgres_conninfo) as conn:
        migrate(conn)
        sync_aliases(conn, [{"source_id": "the-odds-api", "alias": "Boston Celtics", "canonical_name": "Boston Celtics",
                             "sport": "basketball"}], now)
        assert conn.execute("SELECT count(*) FROM silver.teams").fetchone()[0] == 0   # NBA 팀이 아직 없으면 건너뜀
        conn.execute("INSERT INTO silver.teams (canonical_name, created_at) VALUES ('Miami Heat', now())")   # 축구로 잘못 있음
        with pytest.raises(ValueError, match="종목 불일치"):
            sync_aliases(conn, [{"source_id": "the-odds-api", "alias": "Miami Heat", "canonical_name": "Miami Heat",
                                 "sport": "basketball"}], now)


def test_quotes_read_spreads_and_totals_with_lines():
    ev = event(books=[{"key": "pinnacle", "last_update": "2026-10-22T20:00:00Z", "markets": [
        {"key": "spreads", "outcomes": [{"name": "Boston Celtics", "price": 1.98, "point": -3.5},
                                        {"name": "New York Knicks", "price": 1.93, "point": 3.5}]},
        {"key": "totals", "outcomes": [{"name": "Over", "price": 1.89, "point": 235.5}, {"name": "Under", "price": 1.99, "point": 235.5}]},
        {"key": "spreads", "outcomes": [{"name": "Boston Celtics", "price": 1.9, "point": -3.5},
                                        {"name": "New York Knicks", "price": 1.9, "point": 2.5}]}]}])   # 기준선이 안 맞으면 버림
    rows = nba.quotes(ev, ("spreads", "totals"))
    assert {(r["market"], r["line"], r["selection"], r["price"]) for r in rows} == {
        ("SPREAD", -3.5, "HOME", 1.98), ("SPREAD", -3.5, "AWAY", 1.93), ("TOTAL", 235.5, "OVER", 1.89), ("TOTAL", 235.5, "UNDER", 1.99)}
    assert nba.quotes(ev) == []   # 기본은 승패만
