-- 기준 시각(var as_of)에 알려진 경기별 최신 정보 1건과 팀 이름.
-- 두 소스를 합친다: football-data.org(live) 버전이 그 시점에 있으면 우선 쓰고, 없으면 openfootball 버전을 쓴다.
--   이유: 2026-27 대조에서 openfootball은 끝난 경기 20건을 아직 "예정"으로, 날짜 변경 31건을 옛 날짜로 두고 있었다.
--   football-data.org는 갱신 시각(lastUpdated)까지 주므로 시점 판단도 더 정확하다.
-- 시점 선택은 Silver의 *_known_at 함수에만 맡긴다(규칙을 두 곳에 두지 않기 위해).
-- depends_on: {{ source('silver', 'match_revisions') }}
-- depends_on: {{ source('silver', 'live_match_revisions') }}

with openfootball as (
    select * from silver.match_revisions_known_at('{{ var("as_of") }}'::timestamptz)
),

live as (
    select * from silver.live_match_revisions_known_at('{{ var("as_of") }}'::timestamptz)
    where source_id = 'football-data-org'
)

select
    m.match_id,
    m.competition_code,
    m.season,
    home.canonical_name as home_team,
    away.canonical_name as away_team,
    coalesce(live.revision_no, o.revision_no) as revision_no,
    o.round,
    -- live의 날짜는 UTC 시작 시각을 리그 현지 시간(현재 EPL만: Europe/London)으로 바꾼 값이다.
    coalesce((live.kickoff_utc at time zone 'Europe/London')::date, o.local_date) as local_date,
    coalesce(live.kickoff_utc, o.kickoff_utc) as kickoff_utc,
    case
        when live.match_id is null then o.status
        when live.status = 'FINISHED' then 'FINISHED'
        when live.status in ('IN_PLAY', 'PAUSED') then 'LIVE'
        when live.status in ('SCHEDULED', 'TIMED') then 'SCHEDULED'
        when live.status = 'AWARDED' then 'AWARDED'
        else 'NOT_PLAYED'                                   -- POSTPONED, SUSPENDED, CANCELLED
    end as status,
    case when live.match_id is null then o.ht_home else live.ht_home end as ht_home,
    case when live.match_id is null then o.ht_away else live.ht_away end as ht_away,
    case when live.match_id is null then o.ft_home else live.ft_home end as ft_home,
    case when live.match_id is null then o.ft_away else live.ft_away end as ft_away,
    case when live.match_id is null then 'openfootball' else 'football-data-org' end as fixture_source,
    coalesce(live.received_at, o.received_at) as received_at
from {{ source('silver', 'matches') }} as m
left join openfootball as o on o.match_id = m.match_id
left join live on live.match_id = m.match_id
join {{ source('silver', 'teams') }} as home on home.team_id = m.home_team_id
join {{ source('silver', 'teams') }} as away on away.team_id = m.away_team_id
where o.match_id is not null or live.match_id is not null
