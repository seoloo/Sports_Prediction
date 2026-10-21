-- 5c: 리그별 모델. 백테스트 실행과 평가 보고서에 리그를 붙인다.
-- 기존 기록은 모두 EPL(en.1)이라 기본값으로 채운다. 같은 모델 버전이라도 리그가 다르면 다른 실행이다.

ALTER TABLE modeling.backtest_runs ADD COLUMN competition_code TEXT NOT NULL DEFAULT 'en.1';
ALTER TABLE modeling.evaluation_reports ADD COLUMN competition_code TEXT NOT NULL DEFAULT 'en.1';

-- 실행을 구분하는 키에 리그를 넣는다(제약 이름은 Postgres가 자동으로 붙인 것이라 찾아서 지운다).
DO $$
DECLARE name TEXT;
BEGIN
  SELECT conname INTO name FROM pg_constraint
  WHERE conrelid = 'modeling.backtest_runs'::regclass AND contype = 'u';
  EXECUTE format('ALTER TABLE modeling.backtest_runs DROP CONSTRAINT %I', name);
END $$;
ALTER TABLE modeling.backtest_runs ADD CONSTRAINT backtest_runs_run_key
  UNIQUE (competition_code, model_version, purpose, eval_seasons, dataset_hash);
