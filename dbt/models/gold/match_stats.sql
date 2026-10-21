-- 끝난 경기의 기록(3g). 한 행 = 경기 하나. 지금 소스는 football-data.co.uk 하나뿐이다.
-- 경기 후에 알려지는 기록이라 모델 입력으로 쓸 때는 결과와 같은 시점 규칙을 따른다(대상 경기보다 앞서 끝난 경기의 기록만).
select
    s.match_id,
    s.home_shots,
    s.away_shots,
    s.home_shots_on_target,
    s.away_shots_on_target,
    s.home_red_cards,
    s.away_red_cards,
    s.source_id,
    s.received_at
from {{ ref('stg_match_stats_known') }} s
where s.source_id = 'football-data-co-uk'
