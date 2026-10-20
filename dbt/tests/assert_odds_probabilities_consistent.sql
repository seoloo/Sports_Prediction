-- 1X2 정규화 확률의 정합성. 결과가 한 행이라도 나오면 실패다.
-- 1) 세 확률의 합은 1(반올림 오차 1e-9 허용)
-- 2) 시장 평균(AVG) 배당의 오버라운드는 1~1.2 사이. 1보다 작으면 평균 배당 계산이 잘못된 것이다.
--    (MAX는 북메이커마다 최고 배당을 모은 값이라 1보다 작을 수 있어 검사하지 않는다.)
select match_id, bookmaker, timing, 'probability_sum' as failed_check
from {{ ref('match_odds_1x2') }}
where abs(prob_home + prob_draw + prob_away - 1) > 1e-9
union all
select match_id, bookmaker, timing, 'avg_overround' as failed_check
from {{ ref('match_odds_1x2') }}
where bookmaker = 'AVG' and (overround < 1 or overround > 1.2)
