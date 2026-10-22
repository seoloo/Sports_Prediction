"""NBA 기준 모델(로드맵 5d-c): 무승부가 없는 승패 2지선다. 출력은 홈 승 확률 하나다.

세 모델 모두 같은 방식으로 쓴다(축구 models.py와 같은 약속).
- fit(games, as_of): as_of 시점에 결과가 알려진 경기(호출한 쪽이 골라서 넘김)로 학습한다.
- predict(game): 홈 승 확률(0~1).
- version: 이름 + 코드 버전 + 하이퍼파라미터.

학습 입력 한 경기 = Game(시작 시각, 시즌, 홈, 원정, 홈 득점, 원정 득점, 중립 경기 여부, 홈·원정 연전 여부).
- 중립 경기: 2019-20 버블(2020-07-30 이후 올랜도 한 곳). 홈 이점을 적용하지 않는다.
- 연전(back-to-back): 전날(미국 동부 날짜)에도 경기를 했는가. 일정은 미리 공개되므로 경기 전에 알 수 있다.

모델
- base_rate: 학습 기간 홈 승 비율. 팀을 보지 않는 기준선.
- margin_rating: 점수 차 = 홈 이점 + 홈 팀 등급 − 원정 팀 등급 (+ 연전 효과) + 잡음. 시간 감쇠 가중 ridge 최소제곱으로
  등급을 구하고, 잔차의 표준편차 σ로 홈 승 확률 = Φ(예상 점수 차 / σ). 축구 Poisson과 같은 "팀 전력 회귀" 계열.
- elo: 경기마다 등급을 갱신하는 Elo(점수 차 배수, 시즌 사이 평균 회귀). 학습 경기가 앞쪽부터 늘어나기만 하므로
  이어서 갱신한다(incremental).
numpy와 표준 라이브러리만 쓴다.
"""
import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import NamedTuple

import numpy as np

CODE_VERSION = "5d.1"
NEUTRAL_RIDGE = 0.1   # 홈 이점·연전 계수에 붙는 약한 벌점(팀 등급보다 훨씬 약하게)
MIN_SIGMA = 6.0       # 학습 초기 잔차가 작게 나와 확률이 지나치게 확신하지 않게
ELO_MEAN = 1500.0


class Game(NamedTuple):
    tipoff: datetime
    season: str
    home: str
    away: str
    home_score: int
    away_score: int
    neutral: bool = False
    home_b2b: bool = False
    away_b2b: bool = False


def _phi(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _clip(p: float) -> float:
    return min(max(p, 1e-6), 1 - 1e-6)


@dataclass
class BaseRateModel:
    """학습 기간 홈 승 비율(라플라스 평활)."""
    name: str = "base_rate"
    rate: float = 0.5

    @property
    def params(self) -> dict:
        return {"laplace": 1}

    @property
    def version(self) -> str:
        return f"{self.name}|{CODE_VERSION}|laplace=1"

    def fit(self, games: list[Game], as_of: datetime) -> "BaseRateModel":
        wins = sum(game.home_score > game.away_score for game in games)
        self.rate = (wins + 1) / (len(games) + 2)
        return self

    def predict(self, game: Game) -> float:
        return _clip(self.rate)


@dataclass
class MarginRatingModel:
    """점수 차 회귀 등급. ridge는 팀 등급 축소 강도, half_life_days는 오래된 경기를 잊는 속도(None이면 감쇠 없음)."""
    name: str = "margin_rating"
    ridge: float = 10.0
    half_life_days: float | None = None
    rest_effect: bool = False
    ratings: dict = field(default_factory=dict)
    home_advantage: float = 0.0
    b2b_effect: float = 0.0
    sigma: float = 12.0

    @property
    def params(self) -> dict:
        params = {"ridge": self.ridge, "half_life_days": self.half_life_days}
        if self.rest_effect:
            params["rest_effect"] = True
        return params

    @property
    def version(self) -> str:
        version = f"{self.name}|{CODE_VERSION}|ridge={self.ridge}|half_life_days={self.half_life_days}"
        return version + ("|rest_effect" if self.rest_effect else "")

    def _row(self, game: Game, index: dict) -> np.ndarray:
        row = np.zeros(len(index) + 2)
        row[0] = 0.0 if game.neutral else 1.0
        row[1] = (float(game.away_b2b) - float(game.home_b2b)) if self.rest_effect else 0.0
        if game.home in index:
            row[2 + index[game.home]] += 1.0
        if game.away in index:
            row[2 + index[game.away]] -= 1.0
        return row

    def fit(self, games: list[Game], as_of: datetime) -> "MarginRatingModel":
        teams = sorted({game.home for game in games} | {game.away for game in games})
        index = {team: position for position, team in enumerate(teams)}
        x = np.array([self._row(game, index) for game in games])
        y = np.array([game.home_score - game.away_score for game in games], dtype=float)
        if self.half_life_days:
            ages = np.array([(as_of - game.tipoff).total_seconds() / 86400 for game in games])
            w = 0.5 ** (ages / self.half_life_days)
        else:
            w = np.ones(len(games))
        penalty = np.diag([NEUTRAL_RIDGE, NEUTRAL_RIDGE] + [self.ridge] * len(teams))
        xtw = x.T * w
        beta = np.linalg.solve(xtw @ x + penalty, xtw @ y)
        residual = y - x @ beta
        self.sigma = max(MIN_SIGMA, float(math.sqrt(np.sum(w * residual ** 2) / np.sum(w))))
        self.home_advantage, self.b2b_effect = float(beta[0]), float(beta[1])
        self.ratings = {team: float(beta[2 + position]) for team, position in index.items()}
        return self

    def expected_margin(self, game: Game) -> float:
        margin = 0.0 if game.neutral else self.home_advantage
        if self.rest_effect:
            margin += self.b2b_effect * (float(game.away_b2b) - float(game.home_b2b))
        return margin + self.ratings.get(game.home, 0.0) - self.ratings.get(game.away, 0.0)

    def predict(self, game: Game) -> float:
        return _clip(_phi(self.expected_margin(game) / self.sigma))


@dataclass
class EloModel:
    """Elo. k: 갱신 크기, home: 홈 이점(Elo 점), carryover: 새 시즌 첫 경기에서 평균(1500)으로 되돌리는 비율.

    점수 차 배수 = (|점수 차| + 3)^0.8 / (7.5 + 0.006 × 이긴 쪽 등급 차). 등급이 높은 팀이 크게 이기는 것은 덜 반영한다.
    """
    name: str = "elo"
    k: float = 20.0
    home: float = 100.0
    carryover: float = 0.25
    ratings: dict = field(default_factory=dict)
    last_season: dict = field(default_factory=dict)
    seen: int = 0
    incremental = True   # walk_forward가 같은 인스턴스에 늘어난 학습 경기만 이어서 넣는다

    @property
    def params(self) -> dict:
        return {"k": self.k, "home": self.home, "carryover": self.carryover}

    @property
    def version(self) -> str:
        return f"{self.name}|{CODE_VERSION}|k={self.k}|home={self.home}|carryover={self.carryover}"

    def _rating(self, team: str, season: str) -> float:
        rating = self.ratings.get(team, ELO_MEAN)
        if team in self.last_season and self.last_season[team] != season:
            rating = ELO_MEAN + (rating - ELO_MEAN) * (1 - self.carryover)
        return rating

    def _home_probability(self, home: float, away: float, neutral: bool) -> float:
        return 1 / (1 + 10 ** (-(home + (0.0 if neutral else self.home) - away) / 400))

    def fit(self, games: list[Game], as_of: datetime) -> "EloModel":
        if len(games) < self.seen:
            raise ValueError("Elo 학습 경기는 앞쪽부터 늘어나기만 해야 합니다")
        for game in games[self.seen:]:
            home, away = self._rating(game.home, game.season), self._rating(game.away, game.season)
            p = self._home_probability(home, away, game.neutral)
            margin = game.home_score - game.away_score
            home_won = margin > 0
            winner_edge = (home + (0.0 if game.neutral else self.home) - away) * (1 if home_won else -1)
            multiplier = (abs(margin) + 3) ** 0.8 / (7.5 + 0.006 * winner_edge)
            delta = self.k * multiplier * ((1.0 if home_won else 0.0) - p)
            self.ratings[game.home], self.ratings[game.away] = home + delta, away - delta
            self.last_season[game.home] = self.last_season[game.away] = game.season
        self.seen = len(games)
        return self

    def predict(self, game: Game) -> float:
        return _clip(self._home_probability(self._rating(game.home, game.season), self._rating(game.away, game.season),
                                            game.neutral))


def model_from_params(name: str, params: dict):
    """저장된 실행의 이름·하이퍼파라미터로 같은 모델을 만든다(경기 전 예측, 5d-d). 버전 비교는 부르는 쪽이 한다."""
    if name == "base_rate":
        return BaseRateModel()
    if name in ("margin_rating", "margin_rating_rest"):
        return MarginRatingModel(name=name, ridge=params["ridge"], half_life_days=params["half_life_days"],
                                 rest_effect=bool(params.get("rest_effect")))
    if name == "elo":
        return EloModel(k=params["k"], home=params["home"], carryover=params["carryover"])
    raise ValueError(f"알 수 없는 NBA 모델: {name}")
