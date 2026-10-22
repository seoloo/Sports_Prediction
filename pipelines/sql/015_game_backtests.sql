-- 5d-c: NBA 백테스트 예측. 실행 기록(modeling.backtest_runs)과 평가 보고서는 축구와 같은 표를 쓰고
-- competition_code = 'nba'로 구분한다. 예측은 NBA 경기(silver.games)를 가리키고 확률이 하나(홈 승)뿐이라 표를 따로 둔다.
-- 결과 공개 가정: 시작 4시간 뒤(연장이 여러 번이면 3시간을 넘길 수 있어 축구보다 길게 잡는다).

CREATE TABLE modeling.game_backtest_predictions (
  run_id BIGINT NOT NULL REFERENCES modeling.backtest_runs(run_id),
  game_id BIGINT NOT NULL REFERENCES silver.games(game_id),
  season TEXT NOT NULL,
  tipoff_utc TIMESTAMPTZ NOT NULL,
  fit_cutoff TIMESTAMPTZ NOT NULL,          -- 이 시각에 알려진 결과로만 학습(블록의 첫 경기 시작 시각)
  last_training_tipoff TIMESTAMPTZ,         -- 학습에 쓴 가장 늦은 경기의 시작 시각
  training_games INT NOT NULL CHECK (training_games >= 0),
  status TEXT NOT NULL CHECK (status IN ('PREDICTED', 'WITHHELD')),
  withheld_reason TEXT,
  prob_home NUMERIC(9, 8),
  result TEXT NOT NULL CHECK (result IN ('HOME', 'AWAY')),
  PRIMARY KEY (run_id, game_id),
  CHECK (fit_cutoff <= tipoff_utc),
  CHECK (last_training_tipoff IS NULL OR last_training_tipoff + interval '4 hours' <= fit_cutoff),
  CHECK ((status = 'PREDICTED') = (prob_home IS NOT NULL)),
  CHECK ((status = 'WITHHELD') = (withheld_reason IS NOT NULL)),
  CHECK (prob_home IS NULL OR (prob_home > 0 AND prob_home < 1))
);

CREATE TRIGGER game_backtest_predictions_immutable BEFORE UPDATE OR DELETE ON modeling.game_backtest_predictions
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();
