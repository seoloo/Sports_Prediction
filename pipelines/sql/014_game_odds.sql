-- 5d-b: NBA 배당(The Odds API basketball_nba, 승패 2지선다). 소스 검토는 011(the-odds-api)과 같다.
-- 축구 배당(silver.odds_revisions)은 silver.matches를 가리키므로, silver.games를 가리키는 배당 표를 따로 둔다.
-- 시장은 승패(H2H, 연장 포함 최종 승자)뿐이고 무승부 선택지가 없다.

CREATE TABLE silver.game_odds_revisions (
  game_id BIGINT NOT NULL REFERENCES silver.games(game_id),
  source_id TEXT NOT NULL REFERENCES ingest.sources(source_id),
  bookmaker TEXT NOT NULL,                       -- The Odds API 북메이커 키(예: pinnacle)
  market TEXT NOT NULL CHECK (market IN ('H2H')),
  selection TEXT NOT NULL CHECK (selection IN ('HOME', 'AWAY')),
  revision_no INTEGER NOT NULL CHECK (revision_no >= 1),
  price NUMERIC(10, 4) NOT NULL CHECK (price > 1),
  published_at TIMESTAMPTZ,                      -- 북메이커 시장의 last_update(배당이 그 값이 된 시각)
  received_at TIMESTAMPTZ NOT NULL,
  raw_file_id BIGINT NOT NULL REFERENCES ingest.raw_files(raw_file_id),
  PRIMARY KEY (game_id, source_id, bookmaker, market, selection, revision_no)
);

CREATE TRIGGER game_odds_revisions_immutable BEFORE UPDATE OR DELETE ON silver.game_odds_revisions
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();

-- 기준 시각에 알려진 배당의 최신 버전
CREATE FUNCTION silver.game_odds_known_at(as_of TIMESTAMPTZ)
RETURNS SETOF silver.game_odds_revisions AS $$
  SELECT DISTINCT ON (game_id, source_id, bookmaker, market, selection) *
  FROM silver.game_odds_revisions
  WHERE received_at <= as_of AND (published_at IS NULL OR published_at <= as_of)
  ORDER BY game_id, source_id, bookmaker, market, selection, revision_no DESC
$$ LANGUAGE sql STABLE;
