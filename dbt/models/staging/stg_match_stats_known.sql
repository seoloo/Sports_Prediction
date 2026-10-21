-- 기준 시각(var as_of)에 알려진 경기 기록(슈팅·유효 슈팅·퇴장)의 최신 버전(3g). 시점 선택은 Silver 함수에 맡긴다.
-- depends_on: {{ source('silver', 'match_stats_revisions') }}
select *
from silver.match_stats_known_at('{{ var("as_of") }}'::timestamptz)
