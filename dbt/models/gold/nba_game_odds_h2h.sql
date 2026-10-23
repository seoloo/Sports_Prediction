-- NBA 경기 × 소스 × 북메이커별 승패(H2H) 배당과 정규화 내재 확률(6a). 축구 match_odds_1x2의 2지선다판.
-- 내재 확률 = 1 / 배당, 오버라운드 = 두 값의 합(마진). 정규화 확률 = 내재 확률 / 오버라운드.
-- 두 선택지가 모두 있는 경우만 만든다. 핸디캡·언더오버(SPREAD·TOTAL)는 기준선이 있어 이 표에 넣지 않는다.
-- depends_on: {{ source('silver', 'game_odds_revisions') }}

with odds as (
    select * from silver.game_odds_known_at('{{ var("as_of") }}'::timestamptz)
    where market = 'H2H'
),

pivoted as (
    select
        game_id, source_id, bookmaker,
        max(price) filter (where selection = 'HOME') as price_home,
        max(price) filter (where selection = 'AWAY') as price_away,
        max(received_at) as received_at
    from odds
    group by game_id, source_id, bookmaker
),

complete as (
    select *, 1 / price_home + 1 / price_away as overround
    from pivoted
    where price_home is not null and price_away is not null
)

select
    c.game_id,
    g.season,
    g.home_team,
    g.away_team,
    g.tipoff_utc,
    c.source_id,
    c.bookmaker,
    c.price_home,
    c.price_away,
    round(c.overround, 6) as overround,
    round((1 / c.price_home) / c.overround, 6) as prob_home,
    round((1 / c.price_away) / c.overround, 6) as prob_away,
    c.received_at
from complete as c
join {{ ref('stg_games_known') }} as g on g.game_id = c.game_id
