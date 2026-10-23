-- 끝난 NBA 경기 1건을 팀 관점 2행(홈·원정)으로 펼친다(6a). 축구 team_match_results와 같은 구조.
-- 농구는 무승부가 없어 결과는 W·L뿐이다(Silver CHECK + 아래 테스트 두 겹).

with finished as (
    select * from {{ ref('stg_games_known') }}
    where status = 'FINISHED'
),

team_rows as (
    select game_id, competition_code, season, postseason, local_date, tipoff_utc,
           home_team as team, away_team as opponent, 'HOME' as venue,
           home_score as points_for, away_score as points_against, overtimes
    from finished
    union all
    select game_id, competition_code, season, postseason, local_date, tipoff_utc,
           away_team as team, home_team as opponent, 'AWAY' as venue,
           away_score as points_for, home_score as points_against, overtimes
    from finished
)

select
    *,
    case when points_for > points_against then 'W' else 'L' end as result,
    points_for - points_against as margin
from team_rows
