-- NBA 시즌별 정규 시즌 순위(6a). 기준 시각(var as_of)에 끝나 있던 정규 시즌 경기만(포스트시즌 제외).
-- 순위 기준: 승률 → 득실 차 → 팀 이름(동률 처리를 고정해 다시 계산해도 순위가 같게 한다).
-- 실제 NBA의 동률 규칙(상대 전적, 디비전 우승 등)과 컨퍼런스 구분은 반영하지 않았다(팀의 컨퍼런스를 저장하지 않음).
-- 플레이인·NBA컵 결승은 소스의 postseason 표시를 따른다(시즌마다 다를 수 있음, 5d-a 참고).

with regular as (
    select * from {{ ref('nba_team_game_results') }}
    where not postseason
),

totals as (
    select
        competition_code,
        season,
        team,
        count(*) as played,
        count(*) filter (where result = 'W') as won,
        count(*) filter (where result = 'L') as lost,
        sum(points_for) as points_for,
        sum(points_against) as points_against,
        sum(margin) as point_difference
    from regular
    group by competition_code, season, team
)

select
    *,
    round(won::numeric / played, 4) as win_pct,
    round(point_difference::numeric / played, 2) as avg_margin,
    row_number() over (
        partition by competition_code, season
        order by won::numeric / played desc, point_difference desc, team
    ) as position,
    '{{ var("as_of") }}'::timestamptz as as_of
from totals
