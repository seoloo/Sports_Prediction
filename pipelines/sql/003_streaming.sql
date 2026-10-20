
INSERT INTO ingest.sources VALUES
  ('football-data-org', 'football-data.org API v4', 'free tier terms',
   'https://www.football-data.org/client/register', DATE '2026-10-20', TRUE,
   '무료 12개 대회, 분당 10회. 출처 표기 "Football data provided by the Football-Data.org API". 키는 .env에만. 해지 시 데이터 삭제.'),

-- football-data.org가 알려 준 경기 상태·점수의 버전. published_at = API의 lastUpdated(출처의 갱신 시각).
-- 같은 내용(content_hash)은 건너뛰고, 더 오래된 lastUpdated를 가진 늦은 이벤트는 저장하지 않는다(stream_rejections에 기록).
CREATE TABLE silver.live_match_revisions (
  match_id BIGINT NOT NULL REFERENCES silver.matches(match_id),
  source_id TEXT NOT NULL REFERENCES ingest.sources(source_id),
  revision_no INTEGER NOT NULL CHECK (revision_no >= 1),
  external_match_id TEXT NOT NULL,
  status TEXT NOT NULL,                      -- SCHEDULED, TIMED, IN_PLAY, PAUSED, FINISHED, POSTPONED, SUSPENDED, CANCELLED, AWARDED
  kickoff_utc TIMESTAMPTZ,
  matchday INTEGER,
  ht_home INTEGER CHECK (ht_home >= 0),
  ht_away INTEGER CHECK (ht_away >= 0),
  ft_home INTEGER CHECK (ft_home >= 0),
  ft_away INTEGER CHECK (ft_away >= 0),
  duration TEXT,
  published_at TIMESTAMPTZ,
  received_at TIMESTAMPTZ NOT NULL,
  event_id CHAR(64) NOT NULL,
  content_hash CHAR(64) NOT NULL,
  PRIMARY KEY (match_id, source_id, revision_no)
);
CREATE INDEX live_match_revisions_external ON silver.live_match_revisions (source_id, external_match_id);
CREATE INDEX live_match_revisions_event ON silver.live_match_revisions (event_id);

-- 소비자가 반영하지 않은 이벤트와 이유(일종의 dead-letter 테이블). 중복은 정상이라 남기지 않는다.
CREATE TABLE silver.stream_rejections (
  rejection_id BIGSERIAL PRIMARY KEY,
  topic TEXT NOT NULL,
  partition INTEGER NOT NULL,
  "offset" BIGINT NOT NULL,
  event_id TEXT,
  reason TEXT NOT NULL,
  detail JSONB NOT NULL,
  rejected_at TIMESTAMPTZ NOT NULL,
  UNIQUE (topic, partition, "offset", reason)
);

CREATE TRIGGER live_match_revisions_immutable BEFORE UPDATE OR DELETE ON silver.live_match_revisions
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();
CREATE TRIGGER stream_rejections_immutable BEFORE UPDATE OR DELETE ON silver.stream_rejections
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();

CREATE FUNCTION silver.live_match_revisions_known_at(as_of TIMESTAMPTZ)
RETURNS SETOF silver.live_match_revisions AS $$
  SELECT DISTINCT ON (match_id, source_id) *
  FROM silver.live_match_revisions
  WHERE received_at <= as_of AND (published_at IS NULL OR published_at <= as_of)
  ORDER BY match_id, source_id, revision_no DESC
$$ LANGUAGE sql STABLE;
