-- 2e: 경기 시작 전 정해진 시각(24·3·1시간 전)에 그때 알려진 입력을 얼려 둔 스냅샷.
-- PROSPECTIVE(실제로 그 시점에 저장한 기록)라 운영 성능 평가의 근거가 된다. 놓친 시각은 MISSED로 남기고 나중에 채우지 않는다.

CREATE SCHEMA IF NOT EXISTS snapshots;

CREATE TABLE snapshots.prematch_inputs (
  match_id BIGINT NOT NULL REFERENCES silver.matches(match_id),
  offset_label TEXT NOT NULL CHECK (offset_label IN ('T-24H', 'T-3H', 'T-1H')),
  kickoff_utc TIMESTAMPTZ NOT NULL,         -- 결정 당시 알려진 시작 시각
  due_at TIMESTAMPTZ NOT NULL,              -- 스냅샷을 찍어야 했던 시각
  status TEXT NOT NULL CHECK (status IN ('TAKEN', 'MISSED')),
  as_of TIMESTAMPTZ,                        -- 입력을 얼린 시각(TAKEN만)
  inputs JSONB,
  inputs_hash CHAR(64),
  mode TEXT NOT NULL DEFAULT 'PROSPECTIVE',
  recorded_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (match_id, offset_label),
  CHECK ((status = 'TAKEN') = (inputs IS NOT NULL AND as_of IS NOT NULL AND inputs_hash IS NOT NULL))
);

CREATE TRIGGER prematch_inputs_immutable BEFORE UPDATE OR DELETE ON snapshots.prematch_inputs
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();
