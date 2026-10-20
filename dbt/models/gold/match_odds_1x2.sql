-- 경기 × 북메이커 × 시점(마감 전/마감)별 1X2 배당과 내재 확률.
-- 내재 확률 = 1 / 배당. 세 값의 합(오버라운드)은 보통 1보다 크며, 그만큼이 북메이커 마진이다.
-- 정규화 확률 = 내재 확률 / 오버라운드(단순 비례 방식). 실제 확률이라고 단정하지 않고, 모델 평가의 시장 기준선으로 쓴다.
-- 세 선택지가 모두 있는 경우만 만든다. 하나라도 비면 확률을 만들지 않는다.

with odds as (
    select * from {{ ref('stg_odds_known') }}
    where market = '1X2'
),

pivoted as (
    select
        match_id, bookmaker, timing,
        max(price) filter (where selection = 'HOME') as price_home,
        max(price) filter (where selection = 'DRAW') as price_draw,
        max(price) filter (where selection = 'AWAY') as price_away,
        max(caution) as caution
    from odds
    group by match_id, bookmaker, timing
),

complete as (
    select *, 1 / price_home + 1 / price_draw + 1 / price_away as overround
    from pivoted
    where price_home is not null and price_draw is not null and price_away is not null
)

select
    c.match_id,
    m.competition_code,
    m.season,
    m.home_team,
    m.away_team,
    m.local_date,
    c.bookmaker,
    c.timing,
    c.price_home,
    c.price_draw,
    c.price_away,
    round(c.overround, 6) as overround,
    (1 / c.price_home) / c.overround as prob_home,
    (1 / c.price_draw) / c.overround as prob_draw,
    (1 / c.price_away) / c.overround as prob_away,
    case when m.status <> 'FINISHED' then null
         when m.ft_home > m.ft_away then 'HOME' when m.ft_home = m.ft_away then 'DRAW' else 'AWAY' end as result,
    c.caution
from complete as c
join {{ ref('stg_matches_known') }} as m using (match_id)
