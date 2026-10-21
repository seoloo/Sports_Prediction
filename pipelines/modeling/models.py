"""기준 모델(로드맵 3b): 단순 비율 → Poisson → Dixon-Coles.

세 모델 모두 같은 방식으로 쓴다.
- fit(matches, as_of): as_of 시점에 결과가 알려진 경기(호출한 쪽이 골라서 넘김)로 학습한다.
- predict(home_team, away_team): 홈 승·무·원정 승 확률을 돌려준다(합 = 1).
- version: 모델 이름 + 코드 버전 + 하이퍼파라미터. 예측 기록에 함께 남겨, 어떤 모델이 낸 확률인지 추적한다.

학습 입력 한 경기 = (kickoff_utc, home_team, away_team, ft_home, ft_away).
scipy 없이 numpy만 쓴다. Poisson은 벌점(ridge) 붙은 IRLS로 몇 밀리초에 학습된다.
"""
import math
from dataclasses import dataclass, field
from datetime import datetime

import numpy as np

CODE_VERSION = "3b.1"   # 학습·예측 로직을 바꾸면 올린다. 같은 하이퍼파라미터라도 다른 모델로 기록된다.
MAX_GOALS = 10          # 점수 확률표 크기(0~10골). 11골 이상 확률은 무시할 만큼 작아 정규화로 흡수한다.
RHO_GRID = np.round(np.arange(-0.25, 0.2501, 0.005), 3)   # Dixon-Coles 저득점 보정 계수 후보


def outcome(home_goals: int, away_goals: int) -> int:
    """결과를 0(홈 승)·1(무)·2(원정 승)로."""
    return 0 if home_goals > away_goals else 1 if home_goals == away_goals else 2


@dataclass
class BaseRateModel:
    """학습 기간의 홈 승·무·원정 승 비율을 모든 경기에 똑같이 낸다. 팀을 전혀 보지 않는 가장 단순한 기준선."""
    name: str = "base_rate"
    counts: np.ndarray = field(default_factory=lambda: np.ones(3))

    @property
    def params(self) -> dict:
        return {"laplace": 1}

    @property
    def version(self) -> str:
        return f"{self.name}|{CODE_VERSION}|laplace=1"

    def fit(self, matches: list[tuple], as_of: datetime) -> "BaseRateModel":
        self.counts = np.ones(3)   # 라플라스 평활: 학습 경기가 적어도 0% 확률이 나오지 않게
        for _, _, _, home_goals, away_goals in matches:
            self.counts[outcome(home_goals, away_goals)] += 1
        return self

    def predict(self, home_team: str, away_team: str) -> tuple[float, float, float]:
        probabilities = self.counts / self.counts.sum()
        return tuple(float(value) for value in probabilities)


@dataclass
class PoissonModel:
    """팀별 공격력·수비력 + 홈 이점으로 양 팀 득점을 서로 독립인 Poisson으로 본다(Maher 1982).

    log(홈 기대 득점) = mu + home_advantage + attack[홈] - defence[원정]
    log(원정 기대 득점) = mu + attack[원정] - defence[홈]

    - ridge: 공격력·수비력에 붙는 벌점. 기록이 적은 팀일수록 0(리그 평균) 쪽으로 당겨진다(축소 추정).
      학습 기록이 아예 없는 팀(승격 첫 경기 등)은 정확히 리그 평균으로 예측된다.
    - half_life_days: 오래된 경기의 가중치를 이 기간마다 절반으로 줄인다. None이면 모든 경기 같은 가중치.
    """
    ridge: float = 3.0
    half_life_days: float | None = 180.0
    name: str = "poisson"
    mu: float = 0.0
    home_advantage: float = 0.0
    attack: dict = field(default_factory=dict)
    defence: dict = field(default_factory=dict)

    @property
    def params(self) -> dict:
        return {"ridge": self.ridge, "half_life_days": self.half_life_days}

    @property
    def version(self) -> str:
        return f"{self.name}|{CODE_VERSION}|ridge={self.ridge}|half_life_days={self.half_life_days}"

    def weights(self, matches: list[tuple], as_of: datetime) -> np.ndarray:
        """경기별 시간 가중치. as_of에 가까운 경기일수록 1에 가깝다."""
        if self.half_life_days is None:
            return np.ones(len(matches))
        ages = np.array([(as_of - kickoff).total_seconds() / 86400 for kickoff, *_ in matches])
        return 0.5 ** (np.maximum(ages, 0) / self.half_life_days)

    def fit(self, matches: list[tuple], as_of: datetime) -> "PoissonModel":
        teams = sorted({team for _, home, away, _, _ in matches for team in (home, away)})
        index = {team: position for position, team in enumerate(teams)}
        team_count = len(teams)
        # 열: [mu, home_advantage, attack_0..attack_T-1, defence_0..defence_T-1]
        # 행: 경기마다 홈 득점 1행, 원정 득점 1행
        design = np.zeros((2 * len(matches), 2 + 2 * team_count))
        goals = np.zeros(2 * len(matches))
        for row, (_, home, away, home_goals, away_goals) in enumerate(matches):
            h, a = index[home], index[away]
            design[2 * row, [0, 1, 2 + h, 2 + team_count + a]] = [1, 1, 1, -1]
            design[2 * row + 1, [0, 2 + a, 2 + team_count + h]] = [1, 1, -1]
            goals[2 * row], goals[2 * row + 1] = home_goals, away_goals
        row_weights = np.repeat(self.weights(matches, as_of), 2)
        penalty = np.full(design.shape[1], float(self.ridge))
        penalty[:2] = 0.0   # 평균 득점과 홈 이점에는 벌점을 주지 않는다

        coefficients = np.zeros(design.shape[1])
        coefficients[0] = math.log(max(np.average(goals, weights=row_weights), 1e-6))
        for _ in range(100):   # IRLS(반복 가중 최소제곱): Poisson 로그 선형 모델의 표준 학습법
            linear = design @ coefficients
            expected = np.exp(linear)
            working = linear + (goals - expected) / expected
            weighted = design * (row_weights * expected)[:, None]
            updated = np.linalg.solve(design.T @ weighted + np.diag(penalty), weighted.T @ working)
            converged = np.max(np.abs(updated - coefficients)) < 1e-9
            coefficients = updated
            if converged:
                break
        self.mu, self.home_advantage = float(coefficients[0]), float(coefficients[1])
        self.attack = {team: float(coefficients[2 + i]) for team, i in index.items()}
        self.defence = {team: float(coefficients[2 + team_count + i]) for team, i in index.items()}
        return self

    def expected_goals(self, home_team: str, away_team: str) -> tuple[float, float]:
        """홈·원정 기대 득점. 모르는 팀은 공격력·수비력 0(리그 평균)."""
        attack, defence = self.attack.get, self.defence.get
        home = math.exp(self.mu + self.home_advantage + attack(home_team, 0.0) - defence(away_team, 0.0))
        away = math.exp(self.mu + attack(away_team, 0.0) - defence(home_team, 0.0))
        return home, away

    def score_matrix(self, home_team: str, away_team: str) -> np.ndarray:
        """[홈 득점, 원정 득점] 확률표(0~MAX_GOALS골)."""
        home, away = self.expected_goals(home_team, away_team)
        return np.outer(poisson_pmf(home), poisson_pmf(away))

    def predict(self, home_team: str, away_team: str) -> tuple[float, float, float]:
        return outcome_probabilities(self.score_matrix(home_team, away_team))


@dataclass
class DixonColesModel(PoissonModel):
    """Poisson에 저득점(0:0, 1:0, 0:1, 1:1) 보정을 더한 모델(Dixon & Coles 1997).

    독립 Poisson은 실제보다 0:0·1:1 무승부를 적게 본다. rho로 네 점수의 확률만 조정한다.
    학습은 두 단계 근사다: 먼저 Poisson과 같은 방식으로 공격력·수비력을 구하고, 그 기대 득점을 고정한 채
    가중 우도가 가장 큰 rho를 격자에서 고른다(원 논문은 모든 계수를 함께 최대화).
    """
    name: str = "dixon_coles"
    rho: float = 0.0

    def fit(self, matches: list[tuple], as_of: datetime) -> "DixonColesModel":
        super().fit(matches, as_of)
        weights = self.weights(matches, as_of)
        expected = np.array([self.expected_goals(home, away) for _, home, away, _, _ in matches])
        home_rate, away_rate = expected[:, 0], expected[:, 1]
        home_goals = np.array([match[3] for match in matches])
        away_goals = np.array([match[4] for match in matches])
        best_rho, best_likelihood = 0.0, -math.inf
        for rho in RHO_GRID:
            # 학습 경기 모두에서 보정한 네 점수 확률이 양수여야 쓸 수 있는 rho다.
            if not valid_rho(home_rate, away_rate, rho):
                continue
            factors = tau(home_goals, away_goals, home_rate, away_rate, rho)
            likelihood = float(np.sum(weights * np.log(factors)))   # 0·1골 밖 경기는 log(1) = 0
            if likelihood > best_likelihood:
                best_rho, best_likelihood = float(rho), likelihood
        self.rho = best_rho
        return self

    def score_matrix(self, home_team: str, away_team: str) -> np.ndarray:
        matrix = super().score_matrix(home_team, away_team)
        home, away = self.expected_goals(home_team, away_team)
        for x in (0, 1):
            for y in (0, 1):
                # 학습에 없던 극단적인 조합에서 음수가 되면 0으로 자른다(정규화가 나머지를 맞춤).
                matrix[x, y] *= max(float(tau(np.array(x), np.array(y), home, away, self.rho)), 0.0)
        return matrix


def tau(home_goals, away_goals, home_rate, away_rate, rho: float):
    """Dixon-Coles 저득점 보정 계수(배열 단위). 0·1골 조합 밖은 1."""
    return np.select(
        [(home_goals == 0) & (away_goals == 0), (home_goals == 0) & (away_goals == 1),
         (home_goals == 1) & (away_goals == 0), (home_goals == 1) & (away_goals == 1)],
        [1 - home_rate * away_rate * rho, 1 + home_rate * rho, 1 + away_rate * rho,
         np.broadcast_to(1 - rho, np.shape(home_rate))],
        default=1.0)


def valid_rho(home_rate, away_rate, rho: float) -> bool:
    """보정한 네 점수 확률이 모두 양수인지(확률이 음수가 되는 rho는 쓰지 않는다)."""
    return bool(np.all(1 - home_rate * away_rate * rho > 0) and np.all(1 + home_rate * rho > 0)
                and np.all(1 + away_rate * rho > 0) and 1 - rho > 0)


def poisson_pmf(rate: float) -> np.ndarray:
    """0~MAX_GOALS골의 Poisson 확률."""
    goals = np.arange(MAX_GOALS + 1)
    return np.exp(goals * math.log(rate) - rate - np.array([math.lgamma(k + 1) for k in goals]))


def outcome_probabilities(matrix: np.ndarray) -> tuple[float, float, float]:
    """점수 확률표를 홈 승·무·원정 승 확률로 접는다. 잘린 꼬리만큼 정규화한다."""
    total = matrix.sum()
    home = np.tril(matrix, -1).sum() / total   # 홈 득점 > 원정 득점
    draw = np.trace(matrix) / total
    return float(home), float(draw), float(1 - home - draw)
