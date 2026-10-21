-- 3b: 기준 모델의 시간순(walk-forward) 백테스트 기록.
-- 실행 한 번 = 모델 버전 × 데이터셋 × 평가 시즌. 같은 조합을 다시 돌리면 새로 쓰지 않고 기존 기록을 쓴다.
-- 모든 예측에 "언제까지의 결과로 학습했는지"(fit_cutoff)를 남기고, 누수가 되는 조합은 CHECK가 막는다.
-- 과거 재구성 가정(HISTORICAL_RECONSTRUCTION)의 결과라 운영 성능(PROSPECTIVE, 3d)과 따로 보고한다.

CREATE SCHEMA IF NOT EXISTS modeling;

CREATE TABLE modeling.backtest_runs (
  run_id BIGSERIAL PRIMARY KEY,
  model_name TEXT NOT NULL,
  model_version TEXT NOT NULL,             -- 이름|코드 버전|하이퍼파라미터
  params JSONB NOT NULL,
  purpose TEXT NOT NULL CHECK (purpose IN ('VALIDATION', 'TEST')),
  eval_seasons TEXT[] NOT NULL,
  dataset_hash CHAR(64) NOT NULL,          -- 학습·평가에 쓴 경기 정보의 해시. 데이터가 바뀌면 다른 실행이 된다
  mode TEXT NOT NULL CHECK (mode = 'HISTORICAL_RECONSTRUCTION'),
  availability_rule TEXT NOT NULL,
  summary JSONB NOT NULL,                  -- 시즌별 경기 수, 로그 손실, Brier, 보류 수
  created_at TIMESTAMPTZ NOT NULL,
  UNIQUE (model_version, purpose, eval_seasons, dataset_hash)
);

CREATE TABLE modeling.backtest_predictions (
  run_id BIGINT NOT NULL REFERENCES modeling.backtest_runs(run_id),
  match_id BIGINT NOT NULL REFERENCES silver.matches(match_id),
  season TEXT NOT NULL,
  kickoff_utc TIMESTAMPTZ NOT NULL,
  fit_cutoff TIMESTAMPTZ NOT NULL,         -- 이 시각에 알려진 결과로만 학습(블록의 첫 경기 시작 시각)
  last_training_kickoff TIMESTAMPTZ,       -- 학습에 쓴 가장 늦은 경기의 시작 시각
  training_matches INT NOT NULL CHECK (training_matches >= 0),
  status TEXT NOT NULL CHECK (status IN ('PREDICTED', 'WITHHELD')),
  withheld_reason TEXT,
  prob_home NUMERIC(9, 8),
  prob_draw NUMERIC(9, 8),
  prob_away NUMERIC(9, 8),
  result TEXT CHECK (result IN ('HOME', 'DRAW', 'AWAY')),
  PRIMARY KEY (run_id, match_id),
  CHECK (fit_cutoff <= kickoff_utc),
  CHECK (last_training_kickoff IS NULL OR last_training_kickoff + interval '3 hours' <= fit_cutoff),
  CHECK ((status = 'PREDICTED') = (prob_home IS NOT NULL AND prob_draw IS NOT NULL AND prob_away IS NOT NULL)),
  CHECK ((status = 'WITHHELD') = (withheld_reason IS NOT NULL)),
  CHECK (status = 'WITHHELD' OR (prob_home > 0 AND prob_draw > 0 AND prob_away > 0
                                 AND abs(prob_home + prob_draw + prob_away - 1) < 0.000001))
);

CREATE TRIGGER backtest_runs_immutable BEFORE UPDATE OR DELETE ON modeling.backtest_runs
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();
CREATE TRIGGER backtest_predictions_immutable BEFORE UPDATE OR DELETE ON modeling.backtest_predictions
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();
