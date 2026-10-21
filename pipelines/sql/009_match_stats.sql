-- 3g: 경기 기록(슈팅·유효 슈팅·퇴장)의 버전. football-data.co.uk CSV(사용자가 직접 내려받은 파일)에서 읽는다.
-- 끝난 경기의 사후 기록이다. 모델은 "시작 + 3시간 뒤 알려짐"(과거 재구성) 또는 실제 수신 시각(경기 전 예측) 이후에만 쓴다.
-- 같은 경기·소스의 값이 바뀐 경우에만 다음 버전을 추가한다(덮어쓰지 않음).

CREATE TABLE silver.match_stats_revisions (
  match_id BIGINT NOT NULL REFERENCES silver.matches(match_id),
  source_id TEXT NOT NULL REFERENCES ingest.sources(source_id),
  revision_no INTEGER NOT NULL CHECK (revision_no >= 1),
  home_shots INTEGER NOT NULL CHECK (home_shots >= 0),
  away_shots INTEGER NOT NULL CHECK (away_shots >= 0),
  home_shots_on_target INTEGER NOT NULL CHECK (home_shots_on_target BETWEEN 0 AND home_shots),
  away_shots_on_target INTEGER NOT NULL CHECK (away_shots_on_target BETWEEN 0 AND away_shots),
  home_red_cards INTEGER NOT NULL CHECK (home_red_cards >= 0),
  away_red_cards INTEGER NOT NULL CHECK (away_red_cards >= 0),
  published_at TIMESTAMPTZ,                      -- 소스가 발표 시각을 주지 않아 비워 둔다
  received_at TIMESTAMPTZ NOT NULL,
  raw_file_id BIGINT NOT NULL REFERENCES ingest.raw_files(raw_file_id),
  PRIMARY KEY (match_id, source_id, revision_no)
);

CREATE TRIGGER match_stats_revisions_immutable BEFORE UPDATE OR DELETE ON silver.match_stats_revisions
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();

-- 기준 시각에 알려진 경기 기록의 최신 버전
CREATE FUNCTION silver.match_stats_known_at(as_of TIMESTAMPTZ)
RETURNS SETOF silver.match_stats_revisions AS $$
  SELECT DISTINCT ON (match_id, source_id) *
  FROM silver.match_stats_revisions
  WHERE received_at <= as_of AND (published_at IS NULL OR published_at <= as_of)
  ORDER BY match_id, source_id, revision_no DESC
$$ LANGUAGE sql STABLE;
