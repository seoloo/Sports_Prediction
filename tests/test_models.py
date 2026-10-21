"""3b 기준 모델 단위 테스트. 알고 있는 계수로 가상 경기를 만들어, 학습이 그 계수를 되찾는지 본다."""
import math
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from pipelines.modeling.models import (CODE_VERSION, BaseRateModel, DixonColesModel, PoissonModel, model_from_params,
                                       promoted_by_season, sharpen)

AS_OF = datetime(2025, 6, 1, tzinfo=timezone.utc)
TRUE_ATTACK = {"A": 0.4, "B": 0.2, "C": 0.0, "D": -0.1, "E": -0.2, "F": -0.3}
TRUE_DEFENCE = {"A": 0.3, "B": 0.1, "C": 0.0, "D": 0.0, "E": -0.1, "F": -0.3}
MU, HOME = 0.2, 0.25


def synthetic_matches(rounds: int, rho: float = 0.0, seed: int = 7) -> list[tuple]:
    """정해진 공격력·수비력·홈 이점(·rho)에서 점수를 뽑은 가상 리그."""
    rng = np.random.default_rng(seed)
    truth = DixonColesModel(ridge=0, half_life_days=None)
    truth.mu, truth.home_advantage, truth.attack, truth.defence, truth.rho = MU, HOME, TRUE_ATTACK, TRUE_DEFENCE, rho
    teams, matches, day = list(TRUE_ATTACK), [], 0
    for _ in range(rounds):
        for home in teams:
            for away in teams:
                if home == away:
                    continue
                matrix = truth.score_matrix(home, away)
                flat = rng.choice(matrix.size, p=(matrix / matrix.sum()).ravel())
                day += 1
                matches.append((AS_OF - timedelta(days=day), home, away, *divmod(int(flat), matrix.shape[1])))
    return matches


def test_poisson_recovers_known_strengths():
    model = PoissonModel(ridge=0.01, half_life_days=None).fit(synthetic_matches(rounds=60), AS_OF)
    assert model.home_advantage == pytest.approx(HOME, abs=0.05)
    # 공격력·수비력은 평균만큼 mu로 옮겨갈 수 있어(식별성), 평균을 뺀 상대값으로 비교한다.
    attack_mean = np.mean(list(model.attack.values()))
    true_mean = np.mean(list(TRUE_ATTACK.values()))
    for team, value in TRUE_ATTACK.items():
        assert model.attack[team] - attack_mean == pytest.approx(value - true_mean, abs=0.08)
    assert max(model.attack, key=model.attack.get) == "A" and min(model.defence, key=model.defence.get) == "F"


def test_unknown_team_is_predicted_as_league_average():
    model = PoissonModel(ridge=3, half_life_days=None).fit(synthetic_matches(rounds=5), AS_OF)
    home, away = model.expected_goals("Promoted FC", "Another New FC")
    assert home == pytest.approx(math.exp(model.mu + model.home_advantage)) and away == pytest.approx(math.exp(model.mu))


def test_ridge_shrinks_strengths_toward_league_average():
    matches = synthetic_matches(rounds=2)
    loose = PoissonModel(ridge=0.01, half_life_days=None).fit(matches, AS_OF)
    strict = PoissonModel(ridge=100, half_life_days=None).fit(matches, AS_OF)
    spread = lambda model: max(model.attack.values()) - min(model.attack.values())
    assert spread(strict) < spread(loose) / 3


def test_time_decay_halves_weight_each_half_life():
    model = PoissonModel(half_life_days=100)
    matches = [(AS_OF, "A", "B", 1, 0), (AS_OF - timedelta(days=100), "A", "B", 1, 0),
               (AS_OF - timedelta(days=200), "A", "B", 1, 0)]
    assert model.weights(matches, AS_OF) == pytest.approx([1.0, 0.5, 0.25])
    assert list(PoissonModel(half_life_days=None).weights(matches, AS_OF)) == [1.0, 1.0, 1.0]


def test_dixon_coles_finds_low_score_correlation():
    matches = synthetic_matches(rounds=150, rho=-0.12, seed=11)
    model = DixonColesModel(ridge=0.01, half_life_days=None).fit(matches, AS_OF)
    assert model.rho == pytest.approx(-0.12, abs=0.05)
    poisson = PoissonModel(ridge=0.01, half_life_days=None).fit(matches, AS_OF)
    assert model.predict("C", "D")[1] > poisson.predict("C", "D")[1]   # 음의 rho는 무승부(0:0, 1:1)를 늘린다


@pytest.mark.parametrize("model", [BaseRateModel(), PoissonModel(), DixonColesModel()])
def test_probabilities_are_valid(model):
    model.fit(synthetic_matches(rounds=3), AS_OF)
    for home, away in [("A", "F"), ("F", "A"), ("C", "Unknown FC")]:
        probabilities = model.predict(home, away)
        assert all(p > 0 for p in probabilities) and sum(probabilities) == pytest.approx(1, abs=1e-9)
    assert model.version.startswith(f"{model.name}|{CODE_VERSION}|")


def test_base_rate_counts_outcomes_with_smoothing():
    matches = [(AS_OF, "A", "B", 2, 0)] * 5 + [(AS_OF, "A", "B", 1, 1)] * 2 + [(AS_OF, "A", "B", 0, 1)]
    assert BaseRateModel().fit(matches, AS_OF).predict("X", "Y") == pytest.approx((6 / 11, 3 / 11, 2 / 11))


def promoted_league(seed: int = 3) -> list[tuple]:
    """매 시즌 승격 팀 P1·P2가 실제로 약한(공격 −0.4, 수비 −0.3) 가상 리그. 승격 표시를 붙인 학습 경기."""
    rng = np.random.default_rng(seed)
    truth = PoissonModel(ridge=0, half_life_days=None)
    truth.mu, truth.home_advantage = MU, HOME
    truth.attack = {team: 0.0 for team in ("A", "B", "C", "D", "P1", "P2")}
    truth.defence = dict(truth.attack)
    truth.attack.update(P1=-0.4, P2=-0.4)
    truth.defence.update(P1=-0.3, P2=-0.3)
    matches, day = [], 0
    for _ in range(40):
        for home in truth.attack:
            for away in truth.attack:
                if home == away:
                    continue
                matrix = truth.score_matrix(home, away)
                flat = rng.choice(matrix.size, p=(matrix / matrix.sum()).ravel())
                day += 1
                matches.append((AS_OF - timedelta(days=day), home, away, *divmod(int(flat), matrix.shape[1]),
                                home.startswith("P"), away.startswith("P")))
    return matches


def test_promoted_effect_learns_that_promoted_teams_are_weaker():
    matches = promoted_league()
    # 승격 팀의 개별 기록은 빼고 학습해, "기록 없는 새 승격 팀"을 승격 계수만으로 예측하게 한다.
    training = [match for match in matches if not (match[5] and match[6])]
    model = PoissonModel(ridge=1e6, half_life_days=None, promoted_effect=True).fit(training, AS_OF)
    assert model.promoted_attack < -0.25 and model.promoted_defence < -0.15
    newcomer = model.predict("New FC", "A", home_new=True)[0]
    assert newcomer < PoissonModel(ridge=1e6, half_life_days=None).fit(training, AS_OF).predict("New FC", "A")[0]
    # 승격 효과를 끈 모델은 표시를 받아도 무시한다.
    plain = PoissonModel(ridge=3, half_life_days=None).fit(matches, AS_OF)
    assert plain.predict("P1", "A", home_new=True) == plain.predict("P1", "A")


def test_sharpen_spreads_or_flattens_probabilities():
    assert sharpen((0.5, 0.3, 0.2), 1.0) == (0.5, 0.3, 0.2)
    sharper, flatter = sharpen((0.5, 0.3, 0.2), 1.5), sharpen((0.5, 0.3, 0.2), 0.5)
    assert sharper[0] > 0.5 > flatter[0] and sum(sharper) == pytest.approx(1) and sum(flatter) == pytest.approx(1)


def test_model_from_params_rebuilds_the_same_version():
    for model in (BaseRateModel(), PoissonModel(ridge=10.0, half_life_days=180.0),
                  PoissonModel(name="poisson_promoted_calibrated", ridge=10.0, half_life_days=180.0,
                               promoted_effect=True, temperature=1.04, calibration="cross_fit_by_season"),
                  DixonColesModel(ridge=3.0, half_life_days=None)):
        assert model_from_params(model.name, model.params).version == model.version
    assert PoissonModel(ridge=10.0, half_life_days=180.0).params == {"ridge": 10.0, "half_life_days": 180.0}
    # 계수를 고른 방법은 예측에 영향이 없고 버전에만 남는다.
    with_method = PoissonModel(ridge=10.0, half_life_days=180.0, temperature=1.1, calibration="cross_fit_by_season")
    without = PoissonModel(ridge=10.0, half_life_days=180.0, temperature=1.1)
    assert with_method.version == without.version + "|calibration=cross_fit_by_season"


def test_promoted_by_season_compares_with_the_previous_season():
    fixtures = [("2020-21", "A", "B"), ("2020-21", "C", "D"), ("2021-22", "A", "B"), ("2021-22", "C", "E")]
    assert promoted_by_season(fixtures) == {"2020-21": set(), "2021-22": {"E"}}


def test_shot_weight_learns_from_shots_on_target():
    from pipelines.modeling.models import TrainingMatch
    # A는 골은 운 없게 1골씩이지만 유효 슈팅이 늘 B의 세 배. 슈팅을 섞으면 A를 더 강하게 본다.
    matches = []
    for day in range(1, 61):
        kickoff = AS_OF - timedelta(days=day)
        home, away = ("A", "B") if day % 2 else ("B", "A")
        matches.append(TrainingMatch(kickoff, home, away, 1, 1, home_sot=9 if home == "A" else 3,
                                     away_sot=3 if home == "A" else 9))
    goals_only = PoissonModel(ridge=1, half_life_days=None).fit(matches, AS_OF)
    with_shots = PoissonModel(ridge=1, half_life_days=None, shot_weight=0.5).fit(matches, AS_OF)
    assert abs(goals_only.attack["A"] - goals_only.attack["B"]) < 1e-6
    assert with_shots.attack["A"] > with_shots.attack["B"] + 0.2
    assert "shot_weight=0.5" in with_shots.version and with_shots.params["shot_weight"] == 0.5


def test_red_cards_lower_weight_and_rest_effect_is_learned():
    from pipelines.modeling.models import TrainingMatch, rest_difference
    model = PoissonModel(half_life_days=None, red_card_weight=0.5)
    assert list(model.weights([TrainingMatch(AS_OF, "A", "B", 1, 0, red_cards=1),
                               TrainingMatch(AS_OF, "A", "B", 1, 0)], AS_OF)) == [0.5, 1.0]
    assert rest_difference(10, 3) == 1.0 and rest_difference(3, 4) == pytest.approx(-1 / 7) and rest_difference(None, 3) == 0
    # 더 쉰 쪽(홈·원정을 번갈아)이 늘 2:1로 앞서는 가상 자료 → 홈 이점은 0 근처, 휴식 계수는 양수.
    # 더 쉰 쪽이 늘 홈이면 휴식 효과와 홈 이점을 구분할 수 없어 번갈아 둔다.
    matches = []
    for d in range(1, 81):
        home, away = ("A", "B") if d % 2 else ("B", "A")
        home_rested = d % 4 < 2
        matches.append(TrainingMatch(AS_OF - timedelta(days=d), home, away, 2 if home_rested else 1,
                                     1 if home_rested else 2, home_rest=7 if home_rested else 3,
                                     away_rest=3 if home_rested else 7))
    rested = PoissonModel(ridge=1, half_life_days=None, rest_effect=True).fit(matches, AS_OF)
    assert rested.rest_coefficient > 0.3
    assert rested.predict("A", "B", home_rest=7, away_rest=3)[0] > rested.predict("A", "B", home_rest=3, away_rest=7)[0]


def test_market_blend_and_rebuild():
    from pipelines.modeling.models import MarketBlendModel, blend
    assert blend((0.6, 0.2, 0.2), (0.4, 0.3, 0.3), 1.0) == pytest.approx((0.4, 0.3, 0.3))
    assert blend((0.6, 0.2, 0.2), (0.4, 0.3, 0.3), 0.0) == pytest.approx((0.6, 0.2, 0.2))
    mixed = blend((0.6, 0.2, 0.2), (0.4, 0.3, 0.3), 0.5)
    assert 0.4 < mixed[0] < 0.6 and sum(mixed) == pytest.approx(1)
    model = MarketBlendModel(base=PoissonModel(ridge=10.0, half_life_days=180.0), market_weight=0.7,
                             calibration="cross_fit_by_season")
    rebuilt = model_from_params(model.name, model.params)
    assert rebuilt.version == model.version and "market_weight=0.7" in model.version
    model.fit(synthetic_matches(rounds=3), AS_OF)
    assert model.predict("A", "B") is None                                       # 시장 확률이 없으면 만들지 않는다
    assert sum(model.predict("A", "B", market=(0.5, 0.3, 0.2))) == pytest.approx(1)
