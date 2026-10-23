-- NBA 승패 배당의 정규화 확률(6a): 두 확률의 합 = 1, 오버라운드 ≥ 1(마진이 음수인 배당은 형식 오류로 본다).
select game_id, source_id, bookmaker
from {{ ref('nba_game_odds_h2h') }}
where abs(prob_home + prob_away - 1) > 0.000002 or overround < 1
