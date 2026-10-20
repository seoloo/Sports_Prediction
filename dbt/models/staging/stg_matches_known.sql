-- 기준 시각(var as_of)에 알려진 경기별 최신 버전 1건과 팀 이름.
-- 시점 선택은 Silver의 silver.match_revisions_known_at 함수 하나에만 맡긴다. 규칙을 두 곳에 두지 않기 위해서다.
-- source()로 계보를 남기되, 실제 행은 함수에서 읽는다.
-- depends_on: {{ source('silver', 'match_revisions') }}

select
    m.match_id,
    m.competition_code,
    m.season,
    home.canonical_name as home_team,
    away.canonical_name as away_team,
    r.revision_no,
    r.round,
    r.local_date,
    r.kickoff_utc,
    r.status,
    r.ht_home,
    r.ht_away,
    r.ft_home,
    r.ft_away,
    r.received_at
from silver.match_revisions_known_at('{{ var("as_of") }}'::timestamptz) as r
join {{ source('silver', 'matches') }} as m on m.match_id = r.match_id
join {{ source('silver', 'teams') }} as home on home.team_id = m.home_team_id
join {{ source('silver', 'teams') }} as away on away.team_id = m.away_team_id
