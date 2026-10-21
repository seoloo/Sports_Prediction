-- 시점 누수 검사(3a). 결과가 한 행이라도 나오면 실패다.
-- 1) 특징에 쓴 가장 최근 경기도 "시작 + 3시간 <= 대상 경기 시작"이어야 한다(그 경기 결과가 알려진 뒤).
-- 2) 최근 10경기 수는 0~10, 38경기 수는 0~38, 그리고 10경기 수 <= 38경기 수.
select match_id, 'home_future_result' as failed_check
from {{ ref('match_features_pit') }}
where home_last_feature_kickoff + interval '3 hours' > kickoff_utc
union all
select match_id, 'away_future_result'
from {{ ref('match_features_pit') }}
where away_last_feature_kickoff + interval '3 hours' > kickoff_utc
union all
select match_id, 'window_counts'
from {{ ref('match_features_pit') }}
where home_l10_n not between 0 and 10 or away_l10_n not between 0 and 10
   or home_l38_n not between 0 and 38 or away_l38_n not between 0 and 38
   or home_l10_n > home_l38_n or away_l10_n > away_l38_n
