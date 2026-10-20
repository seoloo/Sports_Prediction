-- 1c: football-data.co.uk 배당 CSV(사용자가 직접 내려받은 파일만).
-- 약관: 개인 사용·리그 경기 예측 목적, 자동 수집 금지 → automated_fetch_allowed = FALSE. 수집기는 URL을 열지 않는다.

INSERT INTO ingest.sources VALUES
  ('football-data-co-uk', 'football-data.co.uk results and odds CSV', 'personal use, league match prediction only',
   'https://www.football-data.co.uk/data.php', DATE '2026-10-20', FALSE,
   '사용자가 직접 내려받은 CSV만 처리. 원본과 경기별 배당은 재배포하지 않는다. Pinnacle 배당은 2025-07-23 이후 갱신 지연 안내가 있음.');

-- 배당의 버전. 한 행 = 경기 × 북메이커 × 시장 × 기준선 × 선택지 × 시점(마감 전/마감).
-- 같은 키의 값이 바뀐 경우에만 다음 버전을 추가한다(덮어쓰지 않음).
CREATE TABLE silver.odds_revisions (
  match_id BIGINT NOT NULL REFERENCES silver.matches(match_id),
  source_id TEXT NOT NULL REFERENCES ingest.sources(source_id),
  bookmaker TEXT NOT NULL,                       -- 예: B365, PS(Pinnacle), BFE(Betfair Exchange), MAX, AVG(시장 최고·평균)
  market TEXT NOT NULL CHECK (market IN ('1X2', 'OU', 'AH')),
  line NUMERIC(6, 2) NOT NULL,                   -- 1X2는 0, 언더오버는 기준 득점(2.5), 아시안 핸디캡은 홈 기준 핸디캡
  selection TEXT NOT NULL CHECK (selection IN ('HOME', 'DRAW', 'AWAY', 'OVER', 'UNDER')),
  timing TEXT NOT NULL CHECK (timing IN ('PRE_MATCH', 'CLOSING')),
  revision_no INTEGER NOT NULL CHECK (revision_no >= 1),
  price NUMERIC(10, 4) NOT NULL CHECK (price > 1),
  caution TEXT,                                  -- 예: PINNACLE_STALE_SINCE_2025_07_23
  published_at TIMESTAMPTZ,                      -- 소스가 배당 확보 시각을 주지 않아 비워 둔다
  received_at TIMESTAMPTZ NOT NULL,
  raw_file_id BIGINT NOT NULL REFERENCES ingest.raw_files(raw_file_id),
  PRIMARY KEY (match_id, source_id, bookmaker, market, line, selection, timing, revision_no)
);

CREATE TRIGGER odds_revisions_immutable BEFORE UPDATE OR DELETE ON silver.odds_revisions
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();

-- 기준 시각에 알려진 배당의 최신 버전
CREATE FUNCTION silver.odds_known_at(as_of TIMESTAMPTZ)
RETURNS SETOF silver.odds_revisions AS $$
  SELECT DISTINCT ON (match_id, source_id, bookmaker, market, line, selection, timing) *
  FROM silver.odds_revisions
  WHERE received_at <= as_of AND (published_at IS NULL OR published_at <= as_of)
  ORDER BY match_id, source_id, bookmaker, market, line, selection, timing, revision_no DESC
$$ LANGUAGE sql STABLE;
