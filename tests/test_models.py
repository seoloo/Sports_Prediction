"""3b 기준 모델 단위 테스트. 알고 있는 계수로 가상 경기를 만들어, 학습이 그 계수를 되찾는지 본다."""
import math
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from pipelines.modeling.models import BaseRateModel, DixonColesModel, PoissonModel

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
    assert model.version.startswith(model.name + "|3b.")


def test_base_rate_counts_outcomes_with_smoothing():
    matches = [(AS_OF, "A", "B", 2, 0)] * 5 + [(AS_OF, "A", "B", 1, 1)] * 2 + [(AS_OF, "A", "B", 0, 1)]
    assert BaseRateModel().fit(matches, AS_OF).predict("X", "Y") == pytest.approx((6 / 11, 3 / 11, 2 / 11))
