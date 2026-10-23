-- 기준 시각(var as_of)에 알려진 NBA 경기별 최신 정보 1건과 팀 이름(6a).
-- 시점 선택은 축구와 같이 Silver의 *_known_at 함수에만 맡긴다(규칙을 두 곳에 두지 않기 위해).
-- depends_on: {{ source('silver', 'game_revisions') }}

with known as (
    select * from silver.game_revisions_known_at('{{ var("as_of") }}'::timestamptz)
)

select
    g.game_id,
    g.competition_code,
    g.season,
    g.postseason,
    home.canonical_name as home_team,
    away.canonical_name as away_team,
    k.revision_no,
    k.local_date,
    k.tipoff_utc,
    k.status,
    k.home_score,
    k.away_score,
    k.overtimes,
    k.received_at
from {{ source('silver', 'games') }} as g
join known as k on k.game_id = g.game_id
join {{ source('silver', 'teams') }} as home on home.team_id = g.home_team_id
join {{ source('silver', 'teams') }} as away on away.team_id = g.away_team_id
