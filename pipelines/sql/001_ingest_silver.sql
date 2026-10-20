-- 1b: 수집 이력(ingest)과 정제 경기 데이터(silver).
-- 원칙: 원본과 버전 행은 덮어쓰지 않고 추가만 한다. 수정·삭제는 트리거로 막는다.

CREATE SCHEMA IF NOT EXISTS ingest;
CREATE SCHEMA IF NOT EXISTS silver;

-- 이력 테이블의 UPDATE·DELETE를 막는 공통 트리거 함수
CREATE OR REPLACE FUNCTION ingest.reject_change() RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION 'immutable history: % on %.%', TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

-- 사용 조건을 검토한 소스 목록. docs/football_data_sources.md의 검토 결과를 기록한다.
CREATE TABLE ingest.sources (
  source_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  license TEXT NOT NULL,
  terms_url TEXT NOT NULL,
  reviewed_on DATE NOT NULL,
  automated_fetch_allowed BOOLEAN NOT NULL,
  note TEXT NOT NULL
);
INSERT INTO ingest.sources VALUES
  ('openfootball', 'openfootball/football.json', 'CC0-1.0',
   'https://github.com/openfootball/football.json', DATE '2026-10-20', TRUE,
   '공개 도메인. 일정·전후반 점수. 발표 시각 없음, 경기 시각은 현지 시간(시간대 표기 없음).');

-- 파이프라인 실행 기록. 상태(RUNNING → SUCCEEDED/FAILED)가 바뀌므로 이 테이블만 수정을 허용한다.
CREATE TABLE ingest.runs (
  run_id UUID PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES ingest.sources(source_id),
  dataset TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  status TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
  stats JSONB,
  error TEXT
);

-- Bronze 원본 파일 목록. 내용(SHA-256)이 같으면 한 번만 저장한다.
CREATE TABLE ingest.raw_files (
  raw_file_id BIGSERIAL PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES ingest.sources(source_id),
  dataset TEXT NOT NULL,
  source_url TEXT NOT NULL,
  sha256 CHAR(64) NOT NULL,
  byte_size INTEGER NOT NULL CHECK (byte_size > 0),
  object_key TEXT NOT NULL,
  first_received_at TIMESTAMPTZ NOT NULL,
  UNIQUE (source_id, dataset, sha256)
);

-- 가져올 때마다 남기는 기록. 같은 내용을 다시 받아도 "언제 다시 확인했는지"가 남는다.
CREATE TABLE ingest.fetches (
  fetch_id BIGSERIAL PRIMARY KEY,
  run_id UUID NOT NULL REFERENCES ingest.runs(run_id),
  raw_file_id BIGINT NOT NULL REFERENCES ingest.raw_files(raw_file_id),
  received_at TIMESTAMPTZ NOT NULL,
  is_new_content BOOLEAN NOT NULL
);

-- 팀. 첫 소스(openfootball)의 이름을 기준 이름으로 쓴다.
CREATE TABLE silver.teams (
  team_id BIGSERIAL PRIMARY KEY,
  canonical_name TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL
);

-- 소스별 팀 이름 → 팀. 다른 소스의 이름은 자동 추측하지 않고 이 표에 명시적으로 등록한다.
CREATE TABLE silver.team_aliases (
  source_id TEXT NOT NULL REFERENCES ingest.sources(source_id),
  alias TEXT NOT NULL,
  team_id BIGINT NOT NULL REFERENCES silver.teams(team_id),
  created_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (source_id, alias)
);

-- 경기의 변하지 않는 정체성. 리그 한 시즌에서 같은 홈·원정 조합은 한 번만 열린다.
CREATE TABLE silver.matches (
  match_id BIGSERIAL PRIMARY KEY,
  competition_code TEXT NOT NULL,
  season TEXT NOT NULL,
  home_team_id BIGINT NOT NULL REFERENCES silver.teams(team_id),
  away_team_id BIGINT NOT NULL REFERENCES silver.teams(team_id),
  created_at TIMESTAMPTZ NOT NULL,
  UNIQUE (competition_code, season, home_team_id, away_team_id),
  CHECK (home_team_id <> away_team_id)
);

-- 경기의 바뀔 수 있는 내용(일정·점수)을 버전으로 쌓는다. 내용이 바뀐 경우에만 새 버전을 추가한다.
CREATE TABLE silver.match_revisions (
  match_id BIGINT NOT NULL REFERENCES silver.matches(match_id),
  revision_no INTEGER NOT NULL CHECK (revision_no >= 1),
  round TEXT,
  local_date DATE NOT NULL,
  local_time TIME,
  local_timezone TEXT NOT NULL,
  timezone_assumed BOOLEAN NOT NULL,
  kickoff_utc TIMESTAMPTZ,
  status TEXT NOT NULL CHECK (status IN ('SCHEDULED', 'FINISHED')),
  ht_home INTEGER CHECK (ht_home >= 0),
  ht_away INTEGER CHECK (ht_away >= 0),
  ft_home INTEGER CHECK (ft_home >= 0),
  ft_away INTEGER CHECK (ft_away >= 0),
  published_at TIMESTAMPTZ,
  received_at TIMESTAMPTZ NOT NULL,
  raw_file_id BIGINT NOT NULL REFERENCES ingest.raw_files(raw_file_id),
  content_hash CHAR(64) NOT NULL,
  PRIMARY KEY (match_id, revision_no),
  CHECK ((ft_home IS NULL) = (ft_away IS NULL)),
  CHECK ((ht_home IS NULL) = (ht_away IS NULL)),
  CHECK (status <> 'FINISHED' OR ft_home IS NOT NULL),
  CHECK (ht_home IS NULL OR ft_home IS NOT NULL)
);

-- 검사를 통과하지 못해 Silver에 넣지 않은 원본 레코드와 이유
CREATE TABLE silver.quality_issues (
  issue_id BIGSERIAL PRIMARY KEY,
  run_id UUID NOT NULL REFERENCES ingest.runs(run_id),
  raw_file_id BIGINT NOT NULL REFERENCES ingest.raw_files(raw_file_id),
  record_index INTEGER NOT NULL,
  issue_code TEXT NOT NULL,
  detail JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TRIGGER raw_files_immutable BEFORE UPDATE OR DELETE ON ingest.raw_files
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();
CREATE TRIGGER fetches_immutable BEFORE UPDATE OR DELETE ON ingest.fetches
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();
CREATE TRIGGER team_aliases_immutable BEFORE UPDATE OR DELETE ON silver.team_aliases
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();
CREATE TRIGGER match_revisions_immutable BEFORE UPDATE OR DELETE ON silver.match_revisions
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();
CREATE TRIGGER quality_issues_immutable BEFORE UPDATE OR DELETE ON silver.quality_issues
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();

-- 기준 시각(as_of)에 알려진 최신 버전을 고르는 보기. 사용 예:
--   SELECT * FROM silver.match_revisions_known_at('2026-01-01T00:00:00Z');
CREATE FUNCTION silver.match_revisions_known_at(as_of TIMESTAMPTZ)
RETURNS SETOF silver.match_revisions AS $$
  SELECT DISTINCT ON (match_id) *
  FROM silver.match_revisions
  WHERE received_at <= as_of AND (published_at IS NULL OR published_at <= as_of)
  ORDER BY match_id, revision_no DESC
$$ LANGUAGE sql STABLE;
