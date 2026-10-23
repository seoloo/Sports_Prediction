-- 5d-d: NBA 경기 전 스냅샷·예측(PROSPECTIVE). 축구(004·007)와 같은 규칙을 2지선다로.
-- - 시작 24·3·1시간 전에 그때 알려진 입력을 얼린다. 놓친 시점은 MISSED로 남기고 나중에 채우지 않는다.
-- - 예측은 스냅샷과 같은 기준 시각(as_of)에만 기록할 수 있다(외래 키). 소급 기록을 막는다.
-- - 검증 기준을 통과하지 못한 모델의 확률은 평가용으로만 저장하고 표시 상태는 WITHHELD다.

CREATE TABLE snapshots.game_prematch_inputs (
  game_id BIGINT NOT NULL REFERENCES silver.games(game_id),
  offset_label TEXT NOT NULL CHECK (offset_label IN ('T-24H', 'T-3H', 'T-1H')),
  tipoff_utc TIMESTAMPTZ NOT NULL,
  due_at TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('TAKEN', 'MISSED')),
  as_of TIMESTAMPTZ,
  inputs JSONB,
  inputs_hash CHAR(64),
  mode TEXT NOT NULL DEFAULT 'PROSPECTIVE' CHECK (mode = 'PROSPECTIVE'),
  recorded_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (game_id, offset_label),
  UNIQUE (game_id, offset_label, as_of),
  CHECK ((status = 'TAKEN') = (inputs IS NOT NULL AND as_of IS NOT NULL AND inputs_hash IS NOT NULL))
);

CREATE TABLE snapshots.game_prematch_predictions (
  game_id BIGINT NOT NULL,
  offset_label TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  model_name TEXT NOT NULL,
  model_version TEXT NOT NULL,
  backtest_run_id BIGINT REFERENCES modeling.backtest_runs(run_id),
  evaluation_report_id BIGINT REFERENCES modeling.evaluation_reports(report_id),
  gate_status TEXT NOT NULL CHECK (gate_status IN ('VALIDATED', 'NOT_VALIDATED', 'NO_REPORT', 'UNKNOWN')),
  training_games INT NOT NULL CHECK (training_games >= 0),
  last_training_tipoff TIMESTAMPTZ,
  training_hash CHAR(64),
  prob_home NUMERIC(9, 8),
  display_status TEXT NOT NULL CHECK (display_status IN ('READY', 'WITHHELD')),
  withheld_reasons TEXT[] NOT NULL DEFAULT '{}',
  mode TEXT NOT NULL DEFAULT 'PROSPECTIVE' CHECK (mode = 'PROSPECTIVE'),
  recorded_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (game_id, offset_label, model_version),
  FOREIGN KEY (game_id, offset_label, as_of) REFERENCES snapshots.game_prematch_inputs (game_id, offset_label, as_of),
  CHECK (last_training_tipoff IS NULL OR last_training_tipoff < as_of),
  CHECK (recorded_at >= as_of AND recorded_at <= as_of + interval '10 minutes'),
  CHECK (prob_home IS NULL OR (prob_home > 0 AND prob_home < 1)),
  CHECK ((display_status = 'WITHHELD') = (cardinality(withheld_reasons) > 0)),
  CHECK (display_status = 'WITHHELD' OR (gate_status = 'VALIDATED' AND prob_home IS NOT NULL))
);

CREATE TRIGGER game_prematch_inputs_immutable BEFORE UPDATE OR DELETE ON snapshots.game_prematch_inputs
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();
CREATE TRIGGER game_prematch_predictions_immutable BEFORE UPDATE OR DELETE ON snapshots.game_prematch_predictions
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();
