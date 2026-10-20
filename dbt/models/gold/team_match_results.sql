-- 끝난 경기 1건을 팀 관점 2행(홈 1행, 원정 1행)으로 펼친다.
-- 순위표·폼(최근 경기) 같은 팀 단위 지표를 같은 규칙으로 계산하기 위한 기본 테이블이다.

with finished as (
    select * from {{ ref('stg_matches_known') }}
    where status = 'FINISHED'
),

team_rows as (
    select match_id, competition_code, season, local_date, kickoff_utc,
           home_team as team, away_team as opponent, 'HOME' as venue,
           ft_home as goals_for, ft_away as goals_against, ht_home as ht_goals_for, ht_away as ht_goals_against
    from finished
    union all
    select match_id, competition_code, season, local_date, kickoff_utc,
           away_team as team, home_team as opponent, 'AWAY' as venue,
           ft_away as goals_for, ft_home as goals_against, ht_away as ht_goals_for, ht_home as ht_goals_against
    from finished
)

select
    *,
    case when goals_for > goals_against then 'W' when goals_for = goals_against then 'D' else 'L' end as result,
    case when goals_for > goals_against then 3 when goals_for = goals_against then 1 else 0 end as points
from team_rows
