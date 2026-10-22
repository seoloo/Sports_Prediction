import random
from datetime import date, datetime, timedelta, timezone

import psycopg
import pytest

from pipelines import nba_snapshots
from pipelines.db import migrate
from pipelines.ingest import balldontlie as bdl
from pipelines.modeling import nba_backtest as nb
from pipelines.modeling.nba_models import MarginRatingModel
from pipelines.notify import alerts
from tests.test_balldontlie import page
from tests.test_pipeline_integration import isolated_settings, query  # noqa: F401 (fixture 재사용)

TEAMS = [{"id": i, "full_name": name} for i, name in
         enumerate(("Boston Celtics", "New York Knicks", "Miami Heat", "Chicago Bulls"), start=1)]
TIPOFF = datetime(2026, 1, 20, 0, 30, tzinfo=timezone.utc)   # 미국 동부 1/19 19:30


def quote(source, bookmaker, selection, price):
    return {"source_id": source, "bookmaker": bookmaker, "market": "H2H", "selection": selection, "price": price}


    pinnacle = [quote("the-odds-api", "pinnacle", "HOME", 1.5), quote("the-odds-api", "pinnacle", "AWAY", 2.8)]
    assert source == "the-odds-api:pinnacle" and fair["HOME"] == pytest.approx((1 / 1.5) / (1 / 1.5 + 1 / 2.8))
    books = [quote("the-odds-api", b, s, p) for b in ("a", "b", "c") for s, p in (("HOME", 1.6), ("AWAY", 2.4))]
    assert result["status"] == "COMPARED" and result["best_selection"] == "AWAY"


@pytest.fixture
def season(isolated_settings):
    """2025-26 끝난 경기 220개 + 2026-01-20 예정 경기 하나, 그 경기의 Pinnacle 배당, 활성 모델 기록."""
    s = isolated_settings
    rng = random.Random(3)
    games, day = [], date(2025, 9, 1)   # 220경기가 예정 경기(1/20) 전에 모두 끝나게
    for i in range(220):
        home, away = rng.sample(TEAMS, 2)
        margin = rng.choice([-1, 1]) * rng.randint(1, 20)
        games.append({"id": 1000 + i, "date": day.isoformat(), "datetime": f"{day.isoformat()}T23:30:00.000Z", "season": 2025,
                      "status": "Final", "status_state": "final", "period": 4, "postseason": False, "postponed": False,
                      "home_team_score": 110 + max(margin, 0), "visitor_team_score": 110 + max(-margin, 0),
                      "home_team": home, "visitor_team": away})
        if i % 2:
            day += timedelta(days=1)
    games.append({"id": 9999, "date": "2026-01-19", "datetime": "2026-01-20T00:30:00.000Z", "season": 2025,
                  "status": "2026-01-20T00:30:00Z", "status_state": "scheduled", "period": 0, "postseason": False,
                  "postponed": False, "home_team_score": 0, "visitor_team_score": 0, "home_team": TEAMS[0], "visitor_team": TEAMS[1]})
    bdl.ingest("2025-26", pages=[page(games)], settings=s, received_at=datetime(2026, 1, 18, tzinfo=timezone.utc))
    with psycopg.connect(s.postgres_conninfo) as conn:
        migrate(conn)
        game_id = conn.execute("SELECT game_id FROM silver.games WHERE external_id = '9999'").fetchone()[0]
        raw = conn.execute("SELECT raw_file_id FROM ingest.raw_files LIMIT 1").fetchone()[0]
        for selection, price in (("HOME", 1.5), ("AWAY", 2.8)):
            conn.execute("""INSERT INTO silver.game_odds_revisions VALUES (%s, 'the-odds-api', 'pinnacle', 'H2H', %s, 1, %s,
                            %s, %s, %s)""", (game_id, selection, price, TIPOFF - timedelta(hours=30), TIPOFF - timedelta(hours=26), raw))
        model = MarginRatingModel(name="margin_rating_rest", ridge=3.0, half_life_days=90.0, rest_effect=True)
        for purpose in ("VALIDATION", "TEST"):   # 활성 모델 기록만(예측 없이)
            nb.store_run(conn, model, purpose, ["2021-22"], "h" * 64, {"overall": {"log_loss": 0.64}}, [])
    return s, game_id


def test_missed_only_is_not_sent_alone(season):
    s, _ = season
    assert nba_snapshots.run(now=TIPOFF + timedelta(minutes=5), settings=s)["MISSED"] == 3
    result = alerts.notify_prematch(now=TIPOFF + timedelta(minutes=10), settings=s, sender=lambda *a: pytest.fail("보내면 안 됨"))
    assert result["status"] == "NOTHING_NEW"


    s, game_id = season
    first = nba_snapshots.run(now=TIPOFF - timedelta(hours=23, minutes=50), settings=s)
    prediction = query(s, """SELECT model_name, prob_home IS NOT NULL, display_status, withheld_reasons, training_games
                             FROM snapshots.game_prematch_predictions""")
    assert prediction == [("margin_rating_rest", True, "WITHHELD", ["MODEL_NOT_VALIDATED"], 220)]   # 확률은 평가용으로만
    assert nba_snapshots.run(now=TIPOFF - timedelta(hours=23, minutes=40), settings=s)["TAKEN"] == 0   # 멱등

    with psycopg.connect(s.postgres_conninfo) as conn:
                                        settings=s, now=TIPOFF - timedelta(hours=4))
                                     TIPOFF + timedelta(minutes=5), settings=s, now=TIPOFF + timedelta(minutes=5))
    assert (entered, late) == ("INSERTED", "REJECTED:POST_TIPOFF")
    nba_snapshots.run(now=TIPOFF - timedelta(hours=2, minutes=55), settings=s)
    assert compared == [("COMPARED", "AWAY", True)]
    # PC가 꺼져 1시간 전 시점을 놓치고 경기가 시작되면 MISSED로만 남는다.
    assert nba_snapshots.run(now=TIPOFF + timedelta(minutes=10), settings=s)["MISSED"] == 1

    sent = []
    result = alerts.notify_prematch(now=TIPOFF + timedelta(minutes=15), settings=s,
                                    sender=lambda text, settings: sent.append(text) or {"status": "SENT", "parts": 1})
    assert result["status"] == "SENT" and len(sent) == 1
    assert "홈 승 " not in sent[0]   # 검증 전 모델의 확률은 보내지 않는다
    assert query(s, "SELECT count(*) FROM notify.sent_messages WHERE notification_key LIKE 'nba:%%'")[0][0] == 3
    again = alerts.notify_prematch(now=TIPOFF + timedelta(minutes=45), settings=s, sender=lambda *a: pytest.fail("중복"))
    assert again["status"] == "NOTHING_NEW"

    final = {"id": 9999, "date": "2026-01-19", "datetime": "2026-01-20T00:30:00.000Z", "season": 2025, "status": "Final",
             "status_state": "final", "period": 4, "postseason": False, "postponed": False, "home_team_score": 101,
             "visitor_team_score": 108, "home_team": TEAMS[0], "visitor_team": TEAMS[1]}
    bdl.ingest("2025-26", pages=[page([final])], settings=s, received_at=TIPOFF + timedelta(hours=4))
    from pipelines.modeling import prospective
    week = TIPOFF.date() - timedelta(days=TIPOFF.weekday())
    before = prospective.run_weekly(now=TIPOFF + timedelta(hours=1), week_start=week, settings=s, store=False)
    summary = prospective.run_weekly(now=TIPOFF + timedelta(days=1), week_start=week, settings=s, store=False)
    assert before["nba"]["week"]["T-24H"] == {"games": 0}                      # 결과를 받기 전에는 채점하지 않는다
    nba = summary["nba"]
    assert nba["snapshots"]["T-1H"] == {"MISSED": 1} and nba["snapshots"]["T-24H"]["TAKEN"] == 1
    week_24 = nba["week"]["T-24H"]
    assert week_24["games"] == 1 and week_24["vs_market_pre"]["games"] == 1 and week_24["log_loss"] > 0
    assert "🏀 NBA" in alerts.weekly_text(summary) and "## NBA" in prospective.render_weekly_markdown(summary)
