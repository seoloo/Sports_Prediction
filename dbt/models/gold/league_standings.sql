-- 리그·시즌별 순위표. 기준 시각(var as_of)에 끝나 있던 경기만 반영한다.
-- 순위 기준: 승점 → 골득실 → 다득점 → 팀 이름(동률 처리를 고정해 다시 계산해도 순위가 같게 한다).
-- 실제 리그의 동률 규칙(승자승 등)은 리그마다 달라 아직 반영하지 않았다.

with totals as (
    select
        competition_code,
        season,
        team,
        count(*) as played,
        count(*) filter (where result = 'W') as won,
        count(*) filter (where result = 'D') as drawn,
        count(*) filter (where result = 'L') as lost,
        sum(goals_for) as goals_for,
        sum(goals_against) as goals_against,
        sum(goals_for) - sum(goals_against) as goal_difference,
        sum(points) as points
    from {{ ref('team_match_results') }}
    group by competition_code, season, team
)

select
    *,
    row_number() over (
        partition by competition_code, season
        order by points desc, goal_difference desc, goals_for desc, team
    ) as position,
    '{{ var("as_of") }}'::timestamptz as as_of
from totals
