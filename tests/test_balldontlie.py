"""5d balldontlie NBA 수집기 테스트. 실제로 호출하지 않는다(가짜 응답). 저장은 Postgres·S3가 있을 때만."""
import io
import json
import urllib.error
from dataclasses import replace
from datetime import date, datetime, timezone

import pytest

from pipelines.ingest import balldontlie as bdl
from pipelines.ingest.rate_limit import RateLimiter
from tests.test_pipeline_integration import isolated_settings, query  # noqa: F401 (fixture 재사용)

KEY = "secret-bdl-key-0123456789abcdef"
CELTICS = {"id": 2, "full_name": "Boston Celtics", "abbreviation": "BOS"}
KNICKS = {"id": 20, "full_name": "New York Knicks", "abbreviation": "NYK"}


def game(game_id, home=CELTICS, away=KNICKS, status="Final", state="final", home_score=110, away_score=104,
         period=4, day="2025-10-22", postseason=False, postponed=False):
    return {"id": game_id, "date": day, "datetime": f"{day}T23:30:00.000Z", "season": 2025, "status": status,
            "status_state": state, "period": period, "time": "", "postseason": postseason, "postponed": postponed,
            "home_team_score": home_score, "visitor_team_score": away_score, "home_team": home, "visitor_team": away}


def page(games, next_cursor=None):
    meta = {"per_page": 100, **({"next_cursor": next_cursor} if next_cursor is not None else {})}
    return json.dumps({"data": games, "meta": meta}).encode()


def quiet_limiter():
    ticks = iter(range(0, 10_000, 20))
    return RateLimiter(bdl.MAX_PER_MINUTE, clock=lambda: next(ticks), sleep=lambda seconds: None)


def test_seasons_follow_the_nba_calendar():
    assert bdl.season_label(2025) == "2025-26" and bdl.season_start_year("2025-26") == 2025
    assert bdl.current_season(datetime(2026, 10, 21, tzinfo=timezone.utc)) == "2026-27"
    assert bdl.current_season(datetime(2027, 6, 10, tzinfo=timezone.utc)) == "2026-27"   # 결승은 6월
    with pytest.raises(ValueError):
        bdl.season_start_year("2025-27")


def test_normalize_keeps_unknown_scores_unknown_and_rejects_ties():
    scheduled, _ = bdl.normalize_game(game(1, status="7:30 pm ET", state="scheduled", home_score=0, away_score=0, period=0), 0)
    assert (scheduled.status, scheduled.home_score, scheduled.away_score) == ("SCHEDULED", None, None)   # 시작 전 0점은 모름
    final, _ = bdl.normalize_game(game(2, period=5), 1)
    assert (final.status, final.home_score, final.away_score, final.overtimes) == ("FINISHED", 110, 104, 1)
    assert final.tipoff_utc == datetime(2025, 10, 22, 23, 30, tzinfo=timezone.utc) and final.local_date == date(2025, 10, 22)
    live, _ = bdl.normalize_game(game(3, status="3rd Qtr", state="in_progress", period=3, home_score=70, away_score=70), 2)
    assert (live.status, live.home_score) == ("IN_PROGRESS", 70)        # 진행 중 동점은 정상
    postponed, _ = bdl.normalize_game(game(4, postponed=True, status="Postponed", state="scheduled"), 3)
    assert (postponed.status, postponed.home_score) == ("POSTPONED", None)
    assert bdl.normalize_game(game(5, home_score=100, away_score=100), 4)[1].issue_code == "TIED_FINAL"
    assert bdl.normalize_game(game(6, home=CELTICS, away=CELTICS), 5)[1].issue_code == "SAME_TEAM"
    assert bdl.normalize_game({"id": 7}, 6)[1].issue_code == "MALFORMED_GAME"


def test_content_hash_ignores_identity_and_tracks_scores():
    first = bdl.normalize_game(game(1), 0)[0]
    assert first.content_hash() == bdl.normalize_game(game(1), 9)[0].content_hash()   # 위치가 달라도 같은 내용
    assert first.content_hash() != bdl.normalize_game(game(1, home_score=111), 0)[0].content_hash()


def test_fetch_season_follows_cursor_with_key_in_header_only():
    responses = {None: page([game(1)], next_cursor=1), "1": page([game(2)])}
    seen = []

    def opener(request, timeout):
        seen.append(request)
        cursor = dict(item.split("=") for item in request.full_url.split("?")[1].split("&")).get("cursor")
        return io.BytesIO(responses[cursor])

    pages = bdl.fetch_season(2025, KEY, limiter=quiet_limiter(), opener=opener)
    assert len(pages) == 2 and [json.loads(p)["data"][0]["id"] for p in pages] == [1, 2]
    assert all(KEY not in r.full_url and r.get_header("Authorization") == KEY for r in seen)
    assert "seasons%5B%5D=2025" in seen[0].full_url and "per_page=100" in seen[0].full_url


def test_rate_limit_is_retried_and_errors_never_leak_the_key():
    calls = []

    def throttled_once(request, timeout):
        calls.append(1)
        if len(calls) == 1:
            raise urllib.error.HTTPError(request.full_url, 429, "Too Many Requests", {}, None)
        return io.BytesIO(page([game(1)]))

    assert len(bdl.fetch_season(2025, KEY, limiter=quiet_limiter(), opener=throttled_once)) == 1 and len(calls) == 2

    def unauthorized(request, timeout):
        raise urllib.error.HTTPError(request.full_url, 401, "Unauthorized", {}, None)

    with pytest.raises(bdl.BalldontlieError) as caught:
        bdl.fetch_season(2025, KEY, limiter=quiet_limiter(), opener=unauthorized)
    assert "401" in str(caught.value) and KEY not in str(caught.value) and caught.value.__cause__ is None


def test_without_key_nothing_runs():
    from pipelines.config import load_settings
    try:
        settings = replace(load_settings(), balldontlie_api_key=None)
    except RuntimeError:
        pytest.skip(".env 없음")
    assert bdl.ingest("2025-26", settings=settings)["status"] == "NOT_CONFIGURED"


def run(settings, pages, at):
    return bdl.ingest("2025-26", pages=pages, settings=settings, received_at=datetime.fromisoformat(at))


def test_ingest_versions_only_changes_and_allows_repeated_pairings(isolated_settings):
    s = isolated_settings
    # 같은 홈·원정이 정규 시즌과 플레이오프에 다시 열린다. 축구 matches 키로는 담을 수 없는 경우.
    scheduled = game(3, status="7:30 pm ET", state="scheduled", home_score=0, away_score=0, period=0, day="2026-04-25",
                     postseason=True)
    pages = [page([game(1), game(2, day="2026-01-10")], next_cursor=2), page([scheduled, game(4, home_score=99, away_score=99)])]
    first = run(s, pages, "2026-04-20T00:00:00+00:00")
    assert (first["games_new"], first["revisions_new"], first["quality_issues"], first["pages"]) == (3, 3, 1, 2)
    assert query(s, "SELECT count(*), count(DISTINCT (home_team_id, away_team_id)) FROM silver.games")[0] == (3, 1)
    assert query(s, "SELECT canonical_name, sport FROM silver.teams ORDER BY 1") == [
        ("Boston Celtics", "basketball"), ("New York Knicks", "basketball")]
    assert query(s, "SELECT count(*) FROM silver.matches")[0][0] == 0            # 축구 테이블과 섞이지 않음

    again = run(s, pages, "2026-04-21T00:00:00+00:00")
    assert (again["new_pages"], again["bronze_uploaded"], again["revisions_new"], again["unchanged"]) == (0, 0, 0, 3)
    assert query(s, "SELECT count(*) FROM silver.quality_issues")[0][0] == 1     # 같은 내용의 문제는 한 번만

    # 플레이오프 경기가 끝나면 그 경기만 새 버전. 바뀐 쪽만 새 원본이다.
    finished = game(3, day="2026-04-25", postseason=True, home_score=98, away_score=101)
    later = [pages[0], page([finished, game(4, home_score=99, away_score=99)])]
    third = run(s, later, "2026-04-26T06:00:00+00:00")
    assert (third["new_pages"], third["revisions_new"]) == (1, 1)
    status_at = """SELECT r.status, r.home_score, r.away_score FROM silver.game_revisions_known_at(%s) r
                   JOIN silver.games g USING (game_id) WHERE g.external_id = '3'"""
    assert query(s, status_at, ("2026-04-25T12:00:00+00:00",)) == [("SCHEDULED", None, None)]   # 그때 알던 것
    assert query(s, status_at, ("2026-04-27T00:00:00+00:00",)) == [("FINISHED", 98, 101)]


def test_basketball_name_never_reuses_a_football_team(isolated_settings):
    s = isolated_settings
    import psycopg
    from pipelines.db import migrate
    with psycopg.connect(s.postgres_conninfo) as conn:
        migrate(conn)
        conn.execute("INSERT INTO silver.teams (canonical_name, created_at) VALUES ('Boston Celtics', now())")
    with pytest.raises(ValueError, match="다른 종목"):
        run(s, [page([game(1)])], "2026-04-20T00:00:00+00:00")
    assert query(s, "SELECT status FROM ingest.runs WHERE source_id='balldontlie'") == [("FAILED",)]
