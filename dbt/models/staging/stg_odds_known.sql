-- 기준 시각(var as_of)에 알려진 배당의 최신 버전. 시점 선택은 Silver 함수 silver.odds_known_at에 맡긴다.
-- depends_on: {{ source('silver', 'odds_revisions') }}
select *
from silver.odds_known_at('{{ var("as_of") }}'::timestamptz)
