-- 순위표의 내부 정합성. 결과가 한 행이라도 나오면 테스트 실패다.
-- 1) 경기 수 = 승 + 무 + 패
-- 2) 승점 = 3 × 승 + 무
-- 3) 리그·시즌 전체의 득점 합 = 실점 합 (한 팀의 득점은 상대의 실점)
-- 4) 리그·시즌 전체의 승 수 = 패 수
select competition_code, season, team, 'row' as failed_check
from {{ ref('league_standings') }}
where played <> won + drawn + lost or points <> 3 * won + drawn
union all
select competition_code, season, null, 'season' as failed_check
from {{ ref('league_standings') }}
group by competition_code, season
having sum(goals_for) <> sum(goals_against) or sum(won) <> sum(lost)
