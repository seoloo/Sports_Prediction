-- 5b: The Odds API(다가올 경기의 해외 배당). 이용약관 검토(2026-10-21):
--   - 상업적 이용 포함 허용(데이터 자체를 주된 상품으로 팔지 않는 한), 파생값 계산·연구·통계 모델 학습 허용
--   - 데이터 보관 기한 없음, 출처 표시 의무 없음, 재판매·재배포(원자료를 남에게 제공) 금지, 키는 비공개 유지
--   - 무료 Starter 월 500크레딧. /odds는 시장 수 × 지역 수 크레딧, /events·/sports는 무료
-- 배당은 silver.odds_revisions에 source_id = the-odds-api로 넣는다. bookmaker = The Odds API 북메이커 키(예: pinnacle),
-- published_at = 북메이커 시장의 last_update(배당이 그 값이 된 시각), received_at = 받은 시각.

INSERT INTO ingest.sources VALUES
  ('the-odds-api', 'The Odds API v4 (h2h, region eu)', 'terms: no resale/redistribution; storage and derived values allowed',
   'https://the-odds-api.com/terms-and-conditions.html', DATE '2026-10-21', TRUE,
   '무료 Starter 월 500크레딧. 경기 시작 24시간·1시간 전 무렵에만 리그별로 조회해 예산 안에서 운영. 키는 .env(THE_ODDS_API_KEY)에만.');
