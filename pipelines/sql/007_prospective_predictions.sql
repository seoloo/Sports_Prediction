-- 3d: 경기 전 스냅샷에 붙이는 모델 예측(PROSPECTIVE)과 주간 보고서.
-- 예측은 스냅샷과 같은 기준 시각(as_of)에만 만들 수 있다(아래 외래 키). 나중에 만든 예측·새 모델의 소급 적용을 막는다.
-- 검증 기준(3c)을 통과하지 못한 모델의 확률은 평가용으로만 저장하고, 표시 상태는 WITHHELD와 사유로 남긴다.

ALTER TABLE snapshots.prematch_inputs ADD CONSTRAINT prematch_inputs_as_of_key UNIQUE (match_id, offset_label, as_of);

CREATE TABLE snapshots.prematch_predictions (
  match_id BIGINT NOT NULL,
  offset_label TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,              -- 스냅샷 기준 시각과 같아야 한다
  model_name TEXT NOT NULL,
  model_version TEXT NOT NULL,             -- 예측을 못 만든 경우 'unavailable'
  backtest_run_id BIGINT REFERENCES modeling.backtest_runs(run_id),
  evaluation_report_id BIGINT REFERENCES modeling.evaluation_reports(report_id),
  gate_status TEXT NOT NULL CHECK (gate_status IN ('VALIDATED', 'NOT_VALIDATED', 'NO_REPORT', 'UNKNOWN')),
  training_matches INT NOT NULL CHECK (training_matches >= 0),
  last_training_kickoff TIMESTAMPTZ,       -- 학습에 쓴 가장 늦은 경기(as_of에 이미 받아 둔 결과)
  training_hash CHAR(64),
  home_history INT NOT NULL,               -- 학습 자료 안의 팀별 경기 수
  away_history INT NOT NULL,
  prob_home NUMERIC(9, 8),
  prob_draw NUMERIC(9, 8),
  prob_away NUMERIC(9, 8),
  display_status TEXT NOT NULL CHECK (display_status IN ('READY', 'WITHHELD')),
  withheld_reasons TEXT[] NOT NULL DEFAULT '{}',
  mode TEXT NOT NULL DEFAULT 'PROSPECTIVE' CHECK (mode = 'PROSPECTIVE'),
  recorded_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (match_id, offset_label, model_version),
  FOREIGN KEY (match_id, offset_label, as_of) REFERENCES snapshots.prematch_inputs (match_id, offset_label, as_of),
  CHECK (last_training_kickoff IS NULL OR last_training_kickoff < as_of),
  CHECK (recorded_at >= as_of AND recorded_at <= as_of + interval '10 minutes'),
  CHECK ((prob_home IS NULL) = (prob_draw IS NULL) AND (prob_home IS NULL) = (prob_away IS NULL)),
  CHECK (prob_home IS NULL OR (prob_home > 0 AND prob_draw > 0 AND prob_away > 0
                               AND abs(prob_home + prob_draw + prob_away - 1) < 0.000001)),
  CHECK ((display_status = 'WITHHELD') = (cardinality(withheld_reasons) > 0)),
  CHECK (display_status = 'WITHHELD' OR (gate_status = 'VALIDATED' AND prob_home IS NOT NULL))
);

CREATE TRIGGER prematch_predictions_immutable BEFORE UPDATE OR DELETE ON snapshots.prematch_predictions
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();

-- 주간 보고서. 같은 주라도 결과 정정 등으로 내용이 바뀌면 새 행으로 쌓는다.
CREATE TABLE modeling.prospective_reports (
  report_id BIGSERIAL PRIMARY KEY,
  week_start DATE NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  content_hash CHAR(64) NOT NULL,
  results JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  UNIQUE (week_start, content_hash)
);

CREATE TRIGGER prospective_reports_immutable BEFORE UPDATE OR DELETE ON modeling.prospective_reports
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();
