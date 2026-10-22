-- 6c: NBA 핸디캡(SPREAD)·언더오버(TOTAL) 배당. 기준선(line)을 키에 넣는다.
-- - SPREAD: line = 홈 팀 핸디캡(예: -2.5 = 홈이 3점 차 이상 이겨야 HOME 적중). 선택지 HOME·AWAY.
-- - TOTAL: line = 총점 기준선(예: 236.5). 선택지 OVER·UNDER.
-- - 승패(H2H)는 line = 0. 기존 행은 모두 H2H라 기본값 0으로 채운다.

ALTER TABLE silver.game_odds_revisions ADD COLUMN line NUMERIC(6, 2) NOT NULL DEFAULT 0;
ALTER TABLE silver.game_odds_revisions DROP CONSTRAINT game_odds_revisions_market_check;
ALTER TABLE silver.game_odds_revisions ADD CONSTRAINT game_odds_revisions_market_check
  CHECK (market IN ('H2H', 'SPREAD', 'TOTAL'));
ALTER TABLE silver.game_odds_revisions DROP CONSTRAINT game_odds_revisions_selection_check;
ALTER TABLE silver.game_odds_revisions ADD CONSTRAINT game_odds_revisions_selection_check
  CHECK ((market IN ('H2H', 'SPREAD') AND selection IN ('HOME', 'AWAY'))
         OR (market = 'TOTAL' AND selection IN ('OVER', 'UNDER')));
ALTER TABLE silver.game_odds_revisions ADD CONSTRAINT game_odds_revisions_line_check
  CHECK (market <> 'H2H' OR line = 0);
ALTER TABLE silver.game_odds_revisions DROP CONSTRAINT game_odds_revisions_pkey;
ALTER TABLE silver.game_odds_revisions ADD PRIMARY KEY (game_id, source_id, bookmaker, market, line, selection, revision_no);

CREATE OR REPLACE FUNCTION silver.game_odds_known_at(as_of TIMESTAMPTZ)
RETURNS SETOF silver.game_odds_revisions AS $$
  SELECT DISTINCT ON (game_id, source_id, bookmaker, market, line, selection) *
  FROM silver.game_odds_revisions
  WHERE received_at <= as_of AND (published_at IS NULL OR published_at <= as_of)
  ORDER BY game_id, source_id, bookmaker, market, line, selection, revision_no DESC
$$ LANGUAGE sql STABLE;
