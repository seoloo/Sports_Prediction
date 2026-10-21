-- 3c: 백테스트 평가 보고서. 어떤 실행(run_ids)을 어떤 방법(method: 부트스트랩 횟수·seed·구간·검증 기준)으로
-- 평가했는지와 결과를 남긴다. 같은 실행·방법이면 다시 쓰지 않고, 기준을 바꾸면 새 보고서가 된다.

CREATE TABLE modeling.evaluation_reports (
  report_id BIGSERIAL PRIMARY KEY,
  run_ids BIGINT[] NOT NULL,
  method_hash CHAR(64) NOT NULL,
  method JSONB NOT NULL,
  results JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  UNIQUE (run_ids, method_hash)
);

CREATE TRIGGER evaluation_reports_immutable BEFORE UPDATE OR DELETE ON modeling.evaluation_reports
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();
