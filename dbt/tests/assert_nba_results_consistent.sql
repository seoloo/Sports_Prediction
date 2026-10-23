-- NBA Gold의 내부 정합성(6a). 결과가 한 행이라도 나오면 테스트 실패다.
-- 1) 끝난 경기는 팀 행이 정확히 2개(홈·원정)이고, 한쪽이 W면 다른 쪽은 L
-- 2) 끝난 경기에 동점이 없다(Silver CHECK와 겹치지만, Gold까지 오는 길에서 깨지지 않았는지 다시 본다)
-- 3) 순위표: 경기 수 = 승 + 패, 시즌 전체 승 수 = 패 수, 득점 합 = 실점 합
select game_id::text as key, 'two_rows_one_winner' as failed_check
from {{ ref('nba_team_game_results') }}
group by game_id
having count(*) <> 2 or count(*) filter (where result = 'W') <> 1
union all
select game_id::text, 'tied_final'
from {{ ref('stg_games_known') }}
where status = 'FINISHED' and home_score = away_score
union all
select season || ':' || team, 'standings_row'
from {{ ref('nba_standings') }}
where played <> won + lost
union all
select season, 'standings_season'
from {{ ref('nba_standings') }}
group by season
having sum(won) <> sum(lost) or sum(points_for) <> sum(points_against)
