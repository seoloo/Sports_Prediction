-- 5d: NBA 경기(balldontlie). 이용약관 검토(2026-10-21, Terms of Service 2026-10-20판):
--   - 합법적으로 받은 데이터의 개인·상업적 이용, 저장·보관(기한 없음), 모델 학습·평가·백테스트 허용, 출처 표시 의무 없음
--   - 금지: 데이터로 대체 스포츠 데이터 API·피드를 만들어 남에게 제공, 공식 인터페이스 밖 자동 수집, 요청 한도 회피, 키 공유
--   - 무료 등급: 분당 5회, teams·players·games 엔드포인트. 키는 .env(BALLDONTLIE_API_KEY)에만.
--
-- 축구 silver.matches에 넣지 않는 이유: matches는 (리그, 시즌, 홈, 원정)이 한 번뿐이라는 축구 리그 규칙을 키로 쓴다.
-- NBA는 같은 홈·원정 조합이 정규 시즌에 1~2번, 플레이오프에 최대 4번 더 열리고 무승부가 없다.
-- 그래서 소스의 경기 ID를 정체성으로 쓰는 별도 테이블에 두고, 축구 모델·dbt·스냅샷과 섞이지 않게 한다.

-- 팀은 공용 표를 쓰되 종목을 표시한다(기존 행은 모두 축구).
ALTER TABLE silver.teams ADD COLUMN sport TEXT NOT NULL DEFAULT 'football'
  CHECK (sport IN ('football', 'basketball'));

INSERT INTO ingest.sources VALUES
  ('balldontlie', 'BALLDONTLIE NBA API v1 (games, free tier)',
   'terms: personal/commercial use, storage, model training allowed; no substitute data feed, no key sharing',
   'https://www.balldontlie.io/terms', DATE '2026-10-21', TRUE,
   '무료 등급 분당 5회. 시즌 단위로 /v1/games를 cursor로 끝까지 받는다. 발표 시각을 주지 않아 published_at은 NULL. 키는 .env(BALLDONTLIE_API_KEY)에만.');

-- 경기의 변하지 않는 정체성: 소스 경기 ID. 홈·원정·시즌·포스트시즌 여부는 처음 본 값으로 고정한다.
CREATE TABLE silver.games (
  game_id BIGSERIAL PRIMARY KEY,
  sport TEXT NOT NULL CHECK (sport IN ('basketball')),
  competition_code TEXT NOT NULL,
  season TEXT NOT NULL,
  source_id TEXT NOT NULL REFERENCES ingest.sources(source_id),
  external_id TEXT NOT NULL,
  postseason BOOLEAN NOT NULL,
  home_team_id BIGINT NOT NULL REFERENCES silver.teams(team_id),
  away_team_id BIGINT NOT NULL REFERENCES silver.teams(team_id),
  created_at TIMESTAMPTZ NOT NULL,
  UNIQUE (source_id, external_id),
  CHECK (home_team_id <> away_team_id)
);

-- 바뀌는 내용(일정·상태·점수). 내용이 바뀐 경우에만 새 버전을 추가한다.
CREATE TABLE silver.game_revisions (
  game_id BIGINT NOT NULL REFERENCES silver.games(game_id),
  revision_no INTEGER NOT NULL CHECK (revision_no >= 1),
  local_date DATE NOT NULL,                  -- 미국 동부 기준 경기 날짜(소스의 date)
  tipoff_utc TIMESTAMPTZ,                    -- 소스의 datetime. 없으면 모름
  status TEXT NOT NULL CHECK (status IN ('SCHEDULED', 'IN_PROGRESS', 'FINISHED', 'POSTPONED')),
  home_score INTEGER CHECK (home_score >= 0),
  away_score INTEGER CHECK (away_score >= 0),
  overtimes INTEGER CHECK (overtimes >= 0),   -- 끝난 경기의 연장 횟수. 소스가 주지 않으면 NULL
  published_at TIMESTAMPTZ,
  received_at TIMESTAMPTZ NOT NULL,
  raw_file_id BIGINT NOT NULL REFERENCES ingest.raw_files(raw_file_id),
  content_hash CHAR(64) NOT NULL,
  PRIMARY KEY (game_id, revision_no),
  CHECK ((home_score IS NULL) = (away_score IS NULL)),
  CHECK (status <> 'FINISHED' OR (home_score IS NOT NULL AND home_score <> away_score)),   -- 농구는 무승부가 없다
  CHECK (status NOT IN ('SCHEDULED', 'POSTPONED') OR home_score IS NULL)
);

CREATE TRIGGER game_revisions_immutable BEFORE UPDATE OR DELETE ON silver.game_revisions
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();

-- 기준 시각(as_of)에 알려진 최신 버전. 사용 예: SELECT * FROM silver.game_revisions_known_at('2026-01-01T00:00:00Z');
CREATE FUNCTION silver.game_revisions_known_at(as_of TIMESTAMPTZ)
RETURNS SETOF silver.game_revisions AS $$
  SELECT DISTINCT ON (game_id) *
  FROM silver.game_revisions
  WHERE received_at <= as_of AND (published_at IS NULL OR published_at <= as_of)
  ORDER BY game_id, revision_no DESC
$$ LANGUAGE sql STABLE;
