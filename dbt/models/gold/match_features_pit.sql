-- 시간순 학습·평가 데이터셋(로드맵 3a). 한 행 = 경기 하나.
-- 특징은 그 경기 시작 전에 "알 수 있었던" 결과만으로 계산한다(point-in-time).
--
-- 과거 재구성 가정(HISTORICAL_RECONSTRUCTION):
--   과거 시즌 결과는 모두 최근에 한꺼번에 수집해서, 실제로 받은 시각으로 거르면 쓸 수 있는 과거가 없다.
--   그래서 "결과는 경기 시작 3시간 뒤에 알려졌다"고 가정한다(docs/football_data_sources.md).
--   즉 경기 M의 특징에는 시작 시각 + 3시간 <= M의 시작 시각인 경기만 들어간다.
--   이 가정은 mode·availability_rule 열에 남기고, 운영(PROSPECTIVE) 평가와 따로 보고한다.
--
-- 특징(팀마다 홈·원정 구분 없이 최근 경기 기준):
--   *_l10_*  최근 10경기 평균 득점·실점·승점, 실제로 쓴 경기 수
--   *_l38_*  최근 38경기(약 한 시즌) 평균 득점·실점
--   *_rest_days  직전 경기 후 쉰 날 수
-- 시장 기준선: 시장 평균(AVG) 배당의 정규화 확률(마감 전·마감). 마감 배당은 경기 직전 정보를 담고 있어
--   모델 입력으로 쓰지 않고 비교 기준으로만 쓴다.

{% set availability_lag = "interval '3 hours'" %}

with matches as (
    select match_id, competition_code, season, kickoff_utc, home_team, away_team, status, ft_home, ft_away
    from {{ ref('stg_matches_known') }}
    where kickoff_utc is not null
),

history as (
    select team, match_id, kickoff_utc, goals_for, goals_against, points
    from {{ ref('team_match_results') }}
    where kickoff_utc is not null
),

sides as (
    select m.match_id, 'home' as side, m.home_team as team, m.kickoff_utc from matches m
    union all
    select m.match_id, 'away' as side, m.away_team as team, m.kickoff_utc from matches m
),

side_features as (
    select
        s.match_id,
        s.side,
        l10.n as l10_n,
        l10.gf as l10_gf,
        l10.ga as l10_ga,
        l10.pts as l10_pts,
        l38.n as l38_n,
        l38.gf as l38_gf,
        l38.ga as l38_ga,
        l10.last_kickoff,
        extract(epoch from (s.kickoff_utc - l10.last_kickoff)) / 86400.0 as rest_days
    from sides s
    left join lateral (
        select count(*) as n, avg(goals_for) as gf, avg(goals_against) as ga, avg(points) as pts,
               max(kickoff_utc) as last_kickoff
        from (
            select * from history h
            where h.team = s.team and h.match_id <> s.match_id
              and h.kickoff_utc + {{ availability_lag }} <= s.kickoff_utc
            order by h.kickoff_utc desc
            limit 10
        ) recent
    ) l10 on true
    left join lateral (
        select count(*) as n, avg(goals_for) as gf, avg(goals_against) as ga
        from (
            select * from history h
            where h.team = s.team and h.match_id <> s.match_id
              and h.kickoff_utc + {{ availability_lag }} <= s.kickoff_utc
            order by h.kickoff_utc desc
            limit 38
        ) season_window
    ) l38 on true
),

market as (
    select match_id,
           max(prob_home) filter (where timing = 'PRE_MATCH') as market_pre_home,
           max(prob_draw) filter (where timing = 'PRE_MATCH') as market_pre_draw,
           max(prob_away) filter (where timing = 'PRE_MATCH') as market_pre_away,
           max(prob_home) filter (where timing = 'CLOSING') as market_close_home,
           max(prob_draw) filter (where timing = 'CLOSING') as market_close_draw,
           max(prob_away) filter (where timing = 'CLOSING') as market_close_away
    from {{ ref('match_odds_1x2') }}
    where bookmaker = 'AVG'
    group by match_id
)

select
    m.match_id,
    m.competition_code,
    m.season,
    m.kickoff_utc,
    m.home_team,
    m.away_team,
    h.l10_n as home_l10_n, round(h.l10_gf, 4) as home_l10_gf, round(h.l10_ga, 4) as home_l10_ga,
    round(h.l10_pts, 4) as home_l10_pts,
    h.l38_n as home_l38_n, round(h.l38_gf, 4) as home_l38_gf, round(h.l38_ga, 4) as home_l38_ga,
    round(h.rest_days::numeric, 2) as home_rest_days,
    a.l10_n as away_l10_n, round(a.l10_gf, 4) as away_l10_gf, round(a.l10_ga, 4) as away_l10_ga,
    round(a.l10_pts, 4) as away_l10_pts,
    a.l38_n as away_l38_n, round(a.l38_gf, 4) as away_l38_gf, round(a.l38_ga, 4) as away_l38_ga,
    round(a.rest_days::numeric, 2) as away_rest_days,
    h.last_kickoff as home_last_feature_kickoff,
    a.last_kickoff as away_last_feature_kickoff,
    mk.market_pre_home, mk.market_pre_draw, mk.market_pre_away,
    mk.market_close_home, mk.market_close_draw, mk.market_close_away,
    m.status,
    m.ft_home,
    m.ft_away,
    case when m.status <> 'FINISHED' then null
         when m.ft_home > m.ft_away then 'HOME' when m.ft_home = m.ft_away then 'DRAW' else 'AWAY' end as result,
    'HISTORICAL_RECONSTRUCTION' as mode,
    'result known at kickoff + 3h' as availability_rule
from matches m
join side_features h on h.match_id = m.match_id and h.side = 'home'
join side_features a on a.match_id = m.match_id and a.side = 'away'
left join market mk on mk.match_id = m.match_id
