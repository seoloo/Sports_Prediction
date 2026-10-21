"""기준 모델(로드맵 3b): 단순 비율 → Poisson → Dixon-Coles. 3e: 승격 첫 시즌 효과, 확률 보정(temperature).

세 모델 모두 같은 방식으로 쓴다.
- fit(matches, as_of): as_of 시점에 결과가 알려진 경기(호출한 쪽이 골라서 넘김)로 학습한다.
- predict(home_team, away_team, home_new=False, away_new=False): 홈 승·무·원정 승 확률(합 = 1).
  home_new·away_new는 그 팀이 이번 시즌 승격 팀인지다(promoted_effect를 켠 모델만 쓴다).
- version: 모델 이름 + 코드 버전 + 하이퍼파라미터. 예측 기록에 함께 남겨, 어떤 모델이 낸 확률인지 추적한다.

학습 입력 한 경기 = TrainingMatch(시작 시각, 홈, 원정, 홈 득점, 원정 득점, [승격 표시, 유효 슈팅, 퇴장, 쉰 날 수]).
위치로도 읽을 수 있는 이름 있는 튜플이라 (시작, 홈, 원정, 홈 득점, 원정 득점) 5칸 튜플도 그대로 받는다.
scipy 없이 numpy만 쓴다. Poisson은 벌점(ridge) 붙은 IRLS로 몇 밀리초에 학습된다.
"""
import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import NamedTuple

import numpy as np

CODE_VERSION = "3e.1"   # 학습·예측 로직을 바꾸면 올린다. 같은 하이퍼파라미터라도 다른 모델로 기록된다.
MAX_GOALS = 10          # 점수 확률표 크기(0~10골). 11골 이상 확률은 무시할 만큼 작아 정규화로 흡수한다.
RHO_GRID = np.round(np.arange(-0.25, 0.2501, 0.005), 3)   # Dixon-Coles 저득점 보정 계수 후보
PROMOTED_RIDGE = 1.0    # 승격·휴식 계수에 붙는 약한 벌점. 학습 초기에 해당 경기가 거의 없을 때 값이 튀지 않게
REST_CAP_DAYS = 7.0     # 쉰 날 수 차이는 ±7일에서 자른다(시즌 첫 경기처럼 몇 달 쉰 경우가 계수를 끌고 가지 않게)


class TrainingMatch(NamedTuple):
    """학습 경기 하나. 앞 5칸은 필수, 나머지는 쓰는 모델만 읽는다(없으면 기본값)."""
    kickoff: datetime
    home: str
    away: str
    home_goals: int
    away_goals: int
    home_new: bool = False          # 그 시즌 승격 팀인가(3e)
    away_new: bool = False
    home_sot: int | None = None     # 유효 슈팅(3g). 기록이 없으면 None → 득점만 쓴다
    away_sot: int | None = None
    red_cards: int = 0              # 그 경기의 퇴장 수(양 팀 합)
    home_rest: float | None = None  # 직전 경기 뒤 쉰 날 수
    away_rest: float | None = None


def as_training(match) -> TrainingMatch:
    """5칸·7칸 튜플이나 TrainingMatch를 TrainingMatch로."""
    return match if isinstance(match, TrainingMatch) else TrainingMatch(*match)


def rest_difference(home_rest, away_rest) -> float:
    """(홈 쉰 날 − 원정 쉰 날)을 ±REST_CAP_DAYS에서 자르고 −1~1로. 어느 쪽이라도 모르면 0."""
    if home_rest is None or away_rest is None:
        return 0.0
    return max(-REST_CAP_DAYS, min(REST_CAP_DAYS, float(home_rest) - float(away_rest))) / REST_CAP_DAYS


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
        for match in matches:
            self.counts[outcome(match[3], match[4])] += 1
        return self

    def predict(self, home_team: str, away_team: str, home_new: bool = False, away_new: bool = False,
                home_rest=None, away_rest=None) -> tuple[float, float, float]:
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
    - promoted_effect(3e): 승격 첫 시즌 팀에 공통으로 붙는 공격·수비 계수를 함께 학습한다.
      ridge만 있으면 기록 없는 승격 팀이 "리그 평균"이 되는데, 실제로는 평균보다 약했다(3c).
      이 계수로 과거 승격 팀들의 실제 성적만큼 출발점을 옮긴다.
    - temperature(3e): 승·무·패 확률을 p^t로 바꾼 뒤 다시 정규화한다. 1보다 크면 확률을 펴고(확신 ↑),
      작으면 가운데로 모은다. 축소 때문에 확률이 눌리는 문제(3c)를 검증 시즌에서 고른 값 하나로 보정한다.
    - calibration(3f): temperature를 어떻게 골랐는지(예: "cross_fit_by_season"). 예측에는 영향이 없고
      버전에만 남는다. 고르는 방법이 다르면 검증 점수의 뜻이 달라지므로 다른 모델로 기록한다.
    - shot_weight(3g): 학습 목표를 (1 − w) × 득점 + w × (유효 슈팅 × 리그 전환율)로 바꾼다. 유효 슈팅은 득점보다
      운에 덜 좌우되는 공격력 지표다. 전환율 = 학습 경기의 총득점 ÷ 총 유효 슈팅. 기록이 없는 경기는 득점만 쓴다.
    - red_card_weight(3g): 퇴장이 나온 경기의 가중치에 곱한다. 한 명이 적은 경기는 원래 전력을 덜 보여 준다.
    - rest_effect(3g): 두 팀 쉰 날 수 차이(±7일, rest_difference)의 계수 하나를 학습해 기대 득점에 더한다.
    """
    ridge: float = 3.0
    half_life_days: float | None = 180.0
    name: str = "poisson"
    promoted_effect: bool = False
    temperature: float = 1.0
    calibration: str | None = None
    shot_weight: float = 0.0
    red_card_weight: float = 1.0
    rest_effect: bool = False
    mu: float = 0.0
    home_advantage: float = 0.0
    promoted_attack: float = 0.0
    promoted_defence: float = 0.0
    rest_coefficient: float = 0.0
    attack: dict = field(default_factory=dict)
    defence: dict = field(default_factory=dict)

    @property
    def params(self) -> dict:
        params = {"ridge": self.ridge, "half_life_days": self.half_life_days}
        if self.promoted_effect:
            params["promoted_effect"] = True
        if self.temperature != 1.0:
            params["temperature"] = self.temperature
        if self.calibration:
            params["calibration"] = self.calibration
        if self.shot_weight:
            params["shot_weight"] = self.shot_weight
        if self.red_card_weight != 1.0:
            params["red_card_weight"] = self.red_card_weight
        if self.rest_effect:
            params["rest_effect"] = True
        return params

    @property
    def version(self) -> str:
        version = f"{self.name}|{CODE_VERSION}|ridge={self.ridge}|half_life_days={self.half_life_days}"
        if self.promoted_effect:
            version += "|promoted_effect"
        if self.temperature != 1.0:
            version += f"|temperature={self.temperature}"
        if self.shot_weight:
            version += f"|shot_weight={self.shot_weight}"
        if self.red_card_weight != 1.0:
            version += f"|red_card_weight={self.red_card_weight}"
        if self.rest_effect:
            version += "|rest_effect"
        if self.calibration:
            version += f"|calibration={self.calibration}"
        return version

    def weights(self, matches: list[tuple], as_of: datetime) -> np.ndarray:
        """경기별 가중치. as_of에 가까운 경기일수록 1에 가깝고, 퇴장이 나온 경기는 red_card_weight를 곱한다."""
        if self.half_life_days is None:
            weights = np.ones(len(matches))
        else:
            ages = np.array([(as_of - match[0]).total_seconds() / 86400 for match in matches])
            weights = 0.5 ** (np.maximum(ages, 0) / self.half_life_days)
        if self.red_card_weight != 1.0:
            weights = weights * np.array([self.red_card_weight if as_training(m).red_cards else 1.0 for m in matches])
        return weights

    def targets(self, matches: list[tuple]) -> np.ndarray:
        """경기마다 [홈 목표, 원정 목표]. shot_weight가 0이면 득점 그대로."""
        goals = np.array([(match[3], match[4]) for match in matches], dtype=float)
        if not self.shot_weight:
            return goals
        rows = [as_training(match) for match in matches]
        with_stats = [row for row in rows if row.home_sot is not None and row.away_sot is not None]
        shots = sum(row.home_sot + row.away_sot for row in with_stats)
        if not with_stats or shots == 0:
            return goals
        conversion = sum(row.home_goals + row.away_goals for row in with_stats) / shots
        blended = goals.copy()
        for index, row in enumerate(rows):
            if row.home_sot is not None and row.away_sot is not None:
                blended[index] = ((1 - self.shot_weight) * goals[index]
                                  + self.shot_weight * conversion * np.array([row.home_sot, row.away_sot]))
        return blended

    def flags(self, match: tuple) -> tuple[bool, bool]:
        """학습 경기의 (홈 승격 팀, 원정 승격 팀). 승격 효과를 끈 모델이나 표시가 없는 경기는 (False, False)."""
        if not self.promoted_effect or len(match) < 7:
            return False, False
        return bool(match[5]), bool(match[6])

    def fit(self, matches: list[tuple], as_of: datetime) -> "PoissonModel":
        teams = sorted({team for match in matches for team in (match[1], match[2])})
        index = {team: position for position, team in enumerate(teams)}
        team_count = len(teams)
        promoted_column = 2 + 2 * team_count
        rest_column = promoted_column + (2 if self.promoted_effect else 0)
        # 열: [mu, home_advantage, attack_0..attack_T-1, defence_0..defence_T-1, (승격 공격, 승격 수비), (휴식)]
        # 행: 경기마다 홈 득점 1행, 원정 득점 1행
        design = np.zeros((2 * len(matches), rest_column + (1 if self.rest_effect else 0)))
        goals = np.zeros(2 * len(matches))
        targets = self.targets(matches)
        for row, match in enumerate(matches):
            home, away = match[1:3]
            home_goals, away_goals = targets[row]
            h, a = index[home], index[away]
            design[2 * row, [0, 1, 2 + h, 2 + team_count + a]] = [1, 1, 1, -1]
            design[2 * row + 1, [0, 2 + a, 2 + team_count + h]] = [1, 1, -1]
            home_new, away_new = self.flags(match)
            if self.promoted_effect:
                # 승격 팀의 득점에는 승격 공격 계수가 더해지고, 상대의 득점에는 승격 수비 계수가 빠진다.
                design[2 * row, promoted_column] += home_new
                design[2 * row, promoted_column + 1] -= away_new
                design[2 * row + 1, promoted_column] += away_new
                design[2 * row + 1, promoted_column + 1] -= home_new
            if self.rest_effect:
                # 더 쉰 팀의 기대 득점이 rest_coefficient만큼 오르고, 상대는 그만큼 내려간다.
                difference = rest_difference(as_training(match).home_rest, as_training(match).away_rest)
                design[2 * row, rest_column] = difference
                design[2 * row + 1, rest_column] = -difference
            goals[2 * row], goals[2 * row + 1] = home_goals, away_goals
        row_weights = np.repeat(self.weights(matches, as_of), 2)
        penalty = np.full(design.shape[1], float(self.ridge))
        penalty[:2] = 0.0   # 평균 득점과 홈 이점에는 벌점을 주지 않는다
        penalty[promoted_column:] = PROMOTED_RIDGE

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
        if self.promoted_effect:
            self.promoted_attack = float(coefficients[promoted_column])
            self.promoted_defence = float(coefficients[promoted_column + 1])
        if self.rest_effect:
            self.rest_coefficient = float(coefficients[rest_column])
        return self

    def expected_goals(self, home_team: str, away_team: str, home_new: bool = False,
                       away_new: bool = False, home_rest=None, away_rest=None) -> tuple[float, float]:
        """홈·원정 기대 득점. 모르는 팀은 공격력·수비력 0(리그 평균), 승격 팀이면 승격 계수를 더한다."""
        attack, defence = self.attack.get, self.defence.get
        home_attack = attack(home_team, 0.0) + (self.promoted_attack if home_new else 0.0)
        home_defence = defence(home_team, 0.0) + (self.promoted_defence if home_new else 0.0)
        away_attack = attack(away_team, 0.0) + (self.promoted_attack if away_new else 0.0)
        away_defence = defence(away_team, 0.0) + (self.promoted_defence if away_new else 0.0)
        rest = self.rest_coefficient * rest_difference(home_rest, away_rest) if self.rest_effect else 0.0
        home = math.exp(self.mu + self.home_advantage + home_attack - away_defence + rest)
        away = math.exp(self.mu + away_attack - home_defence - rest)
        return home, away

    def score_matrix(self, home_team: str, away_team: str, home_new: bool = False,
                     away_new: bool = False, home_rest=None, away_rest=None) -> np.ndarray:
        """[홈 득점, 원정 득점] 확률표(0~MAX_GOALS골)."""
        home, away = self.expected_goals(home_team, away_team, home_new, away_new, home_rest, away_rest)
        return np.outer(poisson_pmf(home), poisson_pmf(away))

    def predict(self, home_team: str, away_team: str, home_new: bool = False, away_new: bool = False,
                home_rest=None, away_rest=None) -> tuple[float, float, float]:
        flags = (home_new, away_new) if self.promoted_effect else (False, False)
        matrix = self.score_matrix(home_team, away_team, *flags, home_rest, away_rest)
        return sharpen(outcome_probabilities(matrix), self.temperature)


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
        expected = np.array([self.expected_goals(match[1], match[2], *self.flags(match)) for match in matches])
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

    def score_matrix(self, home_team: str, away_team: str, home_new: bool = False,
                     away_new: bool = False, home_rest=None, away_rest=None) -> np.ndarray:
        matrix = super().score_matrix(home_team, away_team, home_new, away_new, home_rest, away_rest)
        home, away = self.expected_goals(home_team, away_team, home_new, away_new, home_rest, away_rest)
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


def sharpen(probabilities: tuple[float, float, float], temperature: float) -> tuple[float, float, float]:
    """확률을 p^temperature로 바꾸고 다시 정규화한다. temperature = 1이면 그대로."""
    if temperature == 1.0:
        return probabilities
    powered = [value ** temperature for value in probabilities]
    total = sum(powered)
    return tuple(value / total for value in powered)


def blend(model: tuple, market: tuple, market_weight: float) -> tuple[float, float, float]:
    """시장 확률과 모델 확률의 로그 선형 결합: p ∝ market^w × model^(1 − w). w = 1이면 시장 그대로, 0이면 모델 그대로."""
    combined = [m ** market_weight * p ** (1 - market_weight) for m, p in zip(market, model)]
    total = sum(combined)
    return tuple(value / total for value in combined)


@dataclass
class MarketBlendModel:
    """시장 배당(마진 뺀 확률)을 출발점으로 두고 모델 확률을 섞는다(3g).

    시장에는 부상·선발·이적처럼 이 모델이 모르는 정보가 이미 들어 있다. 이 결합이 시장보다 확실히 나아야
    "모델이 시장에 없는 정보를 더한다"고 말할 수 있다. 시장 확률이 없으면 확률을 만들지 않는다.
    """
    base: object
    market_weight: float
    calibration: str | None = None
    name: str = "market_blend"
    needs_market = True

    @property
    def params(self) -> dict:
        params = {"base": self.base.name, "base_params": self.base.params, "market_weight": self.market_weight}
        if self.calibration:
            params["calibration"] = self.calibration
        return params

    @property
    def version(self) -> str:
        version = f"{self.name}|{CODE_VERSION}|base=({self.base.version})|market_weight={self.market_weight}"
        return version + (f"|calibration={self.calibration}" if self.calibration else "")

    def fit(self, matches: list[tuple], as_of: datetime) -> "MarketBlendModel":
        self.base.fit(matches, as_of)
        return self

    def predict(self, home_team: str, away_team: str, home_new: bool = False, away_new: bool = False,
                home_rest=None, away_rest=None, market: tuple | None = None):
        if market is None:
            return None
        return blend(self.base.predict(home_team, away_team, home_new, away_new, home_rest, away_rest), market,
                     self.market_weight)


def promoted_by_season(fixtures) -> dict[str, set[str]]:
    """(시즌, 홈 팀, 원정 팀) 목록 → 시즌별 승격 팀(직전 시즌 목록에 없던 팀).

    시즌 참가 팀은 일정이 나올 때(시즌 전) 이미 알려지므로 미래 정보가 아니다.
    자료의 첫 시즌은 직전 시즌이 없어 판단하지 않는다(빈 집합).
    """
    teams = {}
    for season, home, away in fixtures:
        teams.setdefault(season, set()).update((home, away))
    seasons = sorted(teams)
    promoted = {seasons[0]: set()} if seasons else {}
    for previous, season in zip(seasons, seasons[1:]):
        promoted[season] = teams[season] - teams[previous]
    return promoted


def model_from_params(name: str, params: dict):
    """저장된 이름·하이퍼파라미터로 같은 설정의 모델을 만든다(백테스트·경기 전 예측 공용)."""
    if name == "base_rate":
        return BaseRateModel()
    if name.startswith("market_blend"):
        return MarketBlendModel(base=model_from_params(params["base"], params["base_params"]),
                                market_weight=float(params["market_weight"]), calibration=params.get("calibration"))
    model_class = DixonColesModel if name.startswith("dixon_coles") else PoissonModel
    return model_class(name=name, ridge=params["ridge"], half_life_days=params["half_life_days"],
                       promoted_effect=bool(params.get("promoted_effect", False)),
                       temperature=float(params.get("temperature", 1.0)), calibration=params.get("calibration"),
                       shot_weight=float(params.get("shot_weight", 0.0)),
                       red_card_weight=float(params.get("red_card_weight", 1.0)),
                       rest_effect=bool(params.get("rest_effect", False)))


def outcome_probabilities(matrix: np.ndarray) -> tuple[float, float, float]:
    """점수 확률표를 홈 승·무·원정 승 확률로 접는다. 잘린 꼬리만큼 정규화한다."""
    total = matrix.sum()
    home = np.tril(matrix, -1).sum() / total   # 홈 득점 > 원정 득점
    draw = np.trace(matrix) / total
    return float(home), float(draw), float(1 - home - draw)
