"""5d-c NBA 모델·백테스트 테스트. 가상 자료로 모델이 아는 값을 되찾는지, 미래 정보가 들어가지 않는지 본다."""
import random
from datetime import date, datetime, timedelta, timezone

import pytest

from pipelines.ingest import balldontlie as bdl
from pipelines.modeling import nba_backtest as nb
from pipelines.modeling.nba_models import BaseRateModel, EloModel, Game, MarginRatingModel
from tests.test_balldontlie import page
from tests.test_pipeline_integration import isolated_settings, query  # noqa: F401 (fixture 재사용)

START = datetime(2021, 10, 19, 23, tzinfo=timezone.utc)
STRENGTH = {"A": 6.0, "B": 2.0, "C": -2.0, "D": -6.0}
HOME = 3.0


def synthetic(n=1200, seed=7, b2b_penalty=0.0):
    rng = random.Random(seed)
    teams = list(STRENGTH)
    games = []
    for i in range(n):
        home, away = rng.sample(teams, 2)
        home_b2b, away_b2b = rng.random() < 0.2, rng.random() < 0.2
        margin = HOME + STRENGTH[home] - STRENGTH[away] + b2b_penalty * (away_b2b - home_b2b) + rng.gauss(0, 12)
        margin = round(margin) or 1   # 농구는 동점으로 끝나지 않는다
        home_score = 110 + max(margin, 0)
        games.append(Game(START + timedelta(hours=12 * i), "2021-22", home, away, home_score, home_score - margin,
                          False, home_b2b, away_b2b))
    return games


def test_margin_rating_recovers_home_advantage_and_order():
    games = synthetic()
    model = MarginRatingModel(ridge=1.0).fit(games, games[-1].tipoff)
    assert model.home_advantage == pytest.approx(HOME, abs=1.2)
    assert sorted(model.ratings, key=model.ratings.get, reverse=True) == ["A", "B", "C", "D"]
    assert model.sigma == pytest.approx(12, abs=1.5)
    strong = Game(games[-1].tipoff, "2021-22", "A", "D", 0, 0)
    assert model.predict(strong) > 0.75 and model.predict(strong._replace(home="D", away="A")) < 0.35
    neutral = strong._replace(neutral=True)
    assert model.predict(neutral) < model.predict(strong)   # 중립 경기에는 홈 이점이 없다


def test_rest_effect_learns_back_to_back_penalty():
    games = synthetic(n=3000, b2b_penalty=4.0)
    model = MarginRatingModel(ridge=1.0, rest_effect=True).fit(games, games[-1].tipoff)
    assert model.b2b_effect == pytest.approx(4.0, abs=1.5)
    game = Game(games[-1].tipoff, "2021-22", "B", "C", 0, 0)
    assert model.predict(game._replace(home_b2b=True)) < model.predict(game) < model.predict(game._replace(away_b2b=True))


def test_elo_incremental_equals_refit_and_regresses_between_seasons():
    games = synthetic(n=400)
    once = EloModel().fit(games, games[-1].tipoff)
    stepwise = EloModel()
    for count in (100, 250, 400):
        stepwise.fit(games[:count], games[count - 1].tipoff)
    assert stepwise.ratings == pytest.approx(once.ratings)
    with pytest.raises(ValueError):
        stepwise.fit(games[:10], games[9].tipoff)   # 학습 경기가 줄어들면 이어서 갱신할 수 없다
    team = max(once.ratings, key=once.ratings.get)
    assert abs(once._rating(team, "2022-23") - 1500) < abs(once._rating(team, "2021-22") - 1500)


def test_base_rate_is_home_win_share():
    games = synthetic(n=200)
    wins = sum(g.home_score > g.away_score for g in games)
    assert BaseRateModel().fit(games, games[-1].tipoff).predict(games[0]) == pytest.approx((wins + 1) / 202)


def rows_from(games):
    return [{"game_id": i, "season": g.season, "postseason": False, "tipoff_utc": g.tipoff, "kickoff_utc": g.tipoff,
             "local_date": g.tipoff.date(), "home_team": g.home, "away_team": g.away, "home_score": g.home_score,
             "away_score": g.away_score, "result": "HOME" if g.home_score > g.away_score else "AWAY",
             "neutral": g.neutral, "home_b2b": g.home_b2b, "away_b2b": g.away_b2b} for i, g in enumerate(games)]


def test_walk_forward_never_trains_on_unknown_results():
    rows = rows_from(synthetic(n=600))
    predictions = nb.walk_forward(rows, lambda: MarginRatingModel(ridge=3.0), ["2021-22"])
    tipoffs = {row["game_id"]: row["tipoff_utc"] for row in rows}
    for item in predictions:
        assert item["fit_cutoff"] <= tipoffs[item["game_id"]]
        if item["last_training_tipoff"] is not None:
            assert item["last_training_tipoff"] + nb.AVAILABILITY_LAG <= item["fit_cutoff"]
    early = [item for item in predictions if item["training_games"] < nb.MIN_TRAINING_GAMES]
    assert early and all(item["prob_home"] is None and item["withheld_reason"] for item in early)
    assert any(item["prob_home"] is not None for item in predictions)


def test_load_games_marks_bubble_and_back_to_back(isolated_settings):
    s = isolated_settings
    from tests.test_balldontlie import CELTICS, KNICKS, game
    heat = {"id": 16, "full_name": "Miami Heat"}
    raw = [game(1, day="2020-08-01"), game(2, home=heat, away=CELTICS, day="2020-08-02"),
           game(3, home=KNICKS, away=heat, day="2020-08-05")]
    for item in raw:
        item["datetime"] = f"{item['date']}T23:00:00.000Z"
    bdl.ingest("2019-20", pages=[page(raw)], settings=s, received_at=datetime(2020, 9, 1, tzinfo=timezone.utc))
    import psycopg
    with psycopg.connect(s.postgres_conninfo) as conn:
        rows, excluded = nb.load_games(conn)
    by_id = {row["game_id"]: row for row in rows}
    second = next(row for row in rows if row["local_date"] == date(2020, 8, 2))
    assert all(row["neutral"] for row in rows)                     # 버블 이후는 중립
    assert (second["home_b2b"], second["away_b2b"]) == (False, True)   # 셀틱스는 전날 경기
    assert excluded == {"no_tipoff": 0} and len(by_id) == 3
