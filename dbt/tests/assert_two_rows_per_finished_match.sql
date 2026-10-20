-- 끝난 경기는 팀 관점에서 정확히 2행(홈·원정)이어야 한다. 빠지거나 겹친 경기가 있으면 실패한다.
select s.match_id, count(t.match_id) as team_rows
from {{ ref('stg_matches_known') }} as s
left join {{ ref('team_match_results') }} as t on t.match_id = s.match_id
where s.status = 'FINISHED'
group by s.match_id
having count(t.match_id) <> 2
