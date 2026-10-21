# 3d. 경기 전 예측 기록, 경기 후 채점, 주간 보고서

작성: 2026-10-21.

## 3단계 진행 상태

| 단위 | 내용 | 상태 |
|---|---|---|
| 3a | 시간순 학습 데이터셋, 두 겹의 누수 검사 | 완료 |
| 3b | 기준 모델 3개, 주 단위 재학습 백테스트 | 완료 |
| 3c | 평가 보고서, 검증 기준(모두 NOT_VALIDATED) | 완료 |
| **3d** | 경기 전 스냅샷에 모델 예측을 함께 기록(PROSPECTIVE), 경기 후 채점, 주간 보고서 DAG | 완료 |

3단계(모델링)가 끝났습니다. 지금부터는 매주 경기가 열릴 때마다 "경기 전에 실제로 남긴 예측"이 자동으로 쌓이고, 매주 화요일 채점됩니다.

## 한 줄 요약

경기 24·3·1시간 전에 스냅샷(2e)을 찍는 순간, 그때까지 실제로 받아 둔 결과만으로 모델을 다시 학습해 예측을 함께 저장합니다. 모델이 검증 기준(3c)을 통과하지 못했으므로 확률은 평가용으로만 남기고 표시 상태는 WITHHELD입니다. 매주 화요일 Airflow가 지난주 경기를 채점해 보고서를 쌓습니다.

## 과거 재구성(3b·3c)과 무엇이 다른가

| | 과거 재구성(HISTORICAL_RECONSTRUCTION) | 경기 전 기록(PROSPECTIVE) |
|---|---|---|
| 언제 예측하나 | 지금, 과거 시점을 가정해서 | 그 시점에 실제로 |
| 학습 자료 | "결과는 시작 3시간 뒤 공개" 가정 | 실제로 받은 시각(received_at)과 발표 시각이 모두 기준 시각 이전인 결과 |
| 수집 지연·장애 | 재현하지 못함 | 그대로 반영(PC가 꺼져 있었으면 MISSED) |
| 쓰임 | 모델 개발·선택 | 운영 성능의 기본 근거(설계 문서 7절) |

## 동작 순서

```text
Airflow prematch_snapshots (30분마다)
 └ snapshots.run(now)
    ├ 시점이 됐으면 입력 스냅샷 저장(TAKEN)                          ← 2e
    └ attach_prediction → prospective.record_for_snapshot          ← 3d
        ├ Predictor(as_of = now)   (같은 실행에서는 한 번만 학습)
        │   ├ active_model: 3b 시험 실행 중 검증 로그 손실이 가장 낮은 모델 + 3c 검증 상태
        │   ├ 학습 자료: as_of에 알려진 끝난 경기(silver *_known_at, 스냅샷과 같은 규칙)
        │   └ 모델 학습(numpy, 수 밀리초)
        ├ 확률 계산, 보류 사유 결정
        └ snapshots.prematch_predictions에 저장(as_of는 스냅샷과 같아야 함)

Airflow weekly_model_report (화요일 06:00 UTC)
 └ prospective.run_weekly → 지난주 스냅샷·예측 집계 + 끝난 경기 채점 → modeling.prospective_reports
```

## 보류 사유

| 사유 | 뜻 |
|---|---|
| `MODEL_NOT_VALIDATED` | 3c 검증 기준을 통과하지 못한 모델. 지금은 모든 예측에 붙는다 |
| `LOW_TEAM_HISTORY(<5)` | 한 팀의 학습 경기가 5개 미만(승격 팀 시즌 초 등) |
| `INSUFFICIENT_TRAINING_DATA(<100)` | 학습 경기가 100개 미만. 확률을 만들지 않는다 |
| `NO_ACTIVE_MODEL` | 3b 백테스트가 없다. 확률을 만들지 않는다 |
| `MODEL_CODE_CHANGED` | 3b 이후 모델 코드 버전이 바뀌어, 지금 코드는 평가받지 않은 코드다. 확률을 만들지 않는다 |
| `PREDICTION_FAILED:<오류 종류>` | 예측 중 오류. 입력 스냅샷은 그대로 저장된다 |

## 설계 결정과 이유

| 결정 | 위치 | 이유 |
|---|---|---|
| 예측은 스냅샷을 찍는 순간에만 | `pipelines/snapshots.py`의 `attach_prediction` | 설계 원칙: 놓친 시점을 채우지 않고, 나중에 만든 모델을 과거 시점에 소급하지 않는다 |
| 같은 기준 시각을 DB가 강제 | `pipelines/sql/007_prospective_predictions.sql`의 외래 키 `(match_id, offset_label, as_of)` | 예측의 as_of가 스냅샷과 1초라도 다르면 저장되지 않는다. 기록 시각도 as_of 뒤 10분 안이어야 한다 |
| 학습 자료는 실제 수신 시각 기준 | `prospective.known_fixtures`(스냅샷의 `KNOWN_FIXTURES_SQL` 재사용) | 운영 예측에는 "시작 3시간 뒤 공개" 가정을 쓰지 않는다. 소스 우선순위도 스냅샷·Gold와 같은 규칙 |
| 활성 모델은 검증 시즌 성적으로 | `active_model` | 시험 성적으로 고르면 3b의 원칙(시험은 한 번만)이 깨진다. 그래서 Dixon-Coles가 아니라 Poisson(검증 0.9850 < 0.9868) |
| 평가받은 코드인지 확인 | `build_model` | 모델 코드가 바뀌어 버전 문자열이 다르면 확률을 만들지 않는다(`MODEL_CODE_CHANGED`) |
| 검증 전 확률도 저장 | `Predictor.predict` | 저장하지 않으면 운영 성능을 평가할 수 없어 영원히 검증을 통과할 수 없다. 대신 표시 상태를 WITHHELD로 하고, DB CHECK가 "검증되지 않은 모델은 READY 불가"를 막는다 |
| 예측 실패가 스냅샷을 잃게 하지 않음 | `record_for_snapshot`(저장점), `attach_prediction`(모듈을 못 불러오면 건너뜀) | 입력 스냅샷이 더 중요한 증거다. 실패도 사유와 함께 기록한다 |
| 채점은 매번 그때 알려진 결과로 | `scored_predictions` | 결과가 정정되면 다음 보고서에 반영. 보고서는 내용이 바뀔 때만 새 행으로 쌓인다 |
| 시장 비교는 스냅샷 안의 배당 | `market_probabilities` | 예측 시점에 알려진 시장 평균 마감 전 배당과 비교해야 공정하다. 없으면 "시장 배당 없음" |
| Airflow 이미지에 numpy | `infra/airflow/Dockerfile` | 스냅샷 DAG가 이제 모델을 학습한다. 코드보다 이미지를 먼저 바꿔, 스냅샷 DAG가 멈추는 순간이 없게 했다 |

## 2026-10-21 확인 결과

| 확인 | 결과 |
|---|---|
| 실제 DB 마이그레이션 | `007_prospective_predictions.sql` 적용 |
| 활성 모델 | `poisson|3b.1|ridge=10.0|half_life_days=180.0`(백테스트 run 19, 평가 보고서 2, NOT_VALIDATED) |
| 지금 시점 학습 자료 | 실제로 받은 끝난 경기 1,970개(가장 늦은 경기 10/19) |
| 저장 없이 예측해 보기 | 이번 주말 경기 예: 아스널-에버턴 홈 승 0.602, 빌라-맨시티 원정 승 0.499. 모두 WITHHELD(`MODEL_NOT_VALIDATED`) |
| Airflow | 이미지 다시 빌드(numpy 2.5.4), 컨테이너 안에서 모듈 불러오기 확인, `weekly_model_report` DAG 등록·켜기·시험 실행 성공 |
| 첫 주간 보고서 | 지난주(10/12 주): 시점 27개 모두 MISSED(PC가 꺼져 있던 기간), 예측 0 |
| 테스트 | 3d 테스트 8개, 전체 297개 통과 |

**첫 실제 기록:** 10/22 19:00 UTC(10/23 04:00 한국 시각), 입스위치-노팅엄전 24시간 전 스냅샷에서 시작합니다. 그때 PC와 Docker가 켜져 있어야 합니다.

## 직접 확인하는 방법

지난주 주간 보고서를 만들어 Markdown으로 보기(DB에도 저장, 같은 내용이면 새로 쓰지 않음):

```bash
.\.venv\Scripts\python.exe -m pipelines.modeling.prospective report --markdown data/reports/weekly.md
```

특정 주(월요일 날짜):

```bash
.\.venv\Scripts\python.exe -m pipelines.modeling.prospective report --week 2026-10-19 --markdown data/reports/2026-10-19.md
```

`data/`는 Git에 올라가지 않는 폴더라 개인 보고서를 두기에 맞습니다.

경기 전 예측 보기(Postgres 접속 도구에서):

```sql
SELECT i.kickoff_utc, p.offset_label, p.as_of, p.model_version, p.prob_home, p.prob_draw, p.prob_away,
       p.display_status, p.withheld_reasons
FROM snapshots.prematch_predictions p JOIN snapshots.prematch_inputs i USING (match_id, offset_label)
ORDER BY i.kickoff_utc DESC, p.offset_label LIMIT 20;
```

Airflow 화면(http://127.0.0.1:58080)에서 `weekly_model_report`, `prematch_snapshots` 실행 기록을 볼 수 있습니다.

테스트:

```bash
.\.venv\Scripts\python.exe -m pytest tests/test_prospective.py tests/test_snapshots.py -q
```

## 테스트가 확인하는 것

| 테스트 | 확인 내용 |
|---|---|
| `test_active_model_is_chosen_by_validation_and_carries_gate` | 백테스트가 없으면 활성 모델 없음, 있으면 검증 상태를 함께 가져옴, 코드 버전이 바뀌면 모델을 만들지 않음 |
| `test_snapshot_predictions_use_only_results_received_by_as_of` | 결과를 받기 전 시점에는 학습 자료가 0, 스냅샷 예측의 as_of가 스냅샷과 같고 확률 합 1, 검증 전이라 WITHHELD |
| `test_validated_model_with_enough_history_is_ready` | 검증을 통과하고 기록이 충분하면 READY |
| `test_prediction_failure_keeps_the_input_snapshot` | 예측이 터져도 입력 스냅샷은 저장되고 실패 사유가 남는다 |
| `test_database_rejects_late_or_mismatched_predictions` | 스냅샷과 다른 기준 시각, 검증 안 된 모델의 READY, 기준 시각 뒤 경기로 학습을 DB가 거부 |
| `test_weekly_report_scores_finished_matches` | 결과를 받기 전에는 채점하지 않고, 받은 뒤 채점, 같은 내용이면 보고서를 새로 쓰지 않음 |
| `test_previous_week_start_is_last_monday` | 화요일·월요일 밤에 돌면 지난주 월요일 |
| `test_market_probabilities_from_snapshot_inputs` | 스냅샷 안 배당에서 마진을 뺀 확률, 배당이 빠지면 None |

## 면접에서 나올 만한 질문

**Q. 백테스트가 있는데 왜 경기 전 기록을 따로 쌓나요?**
백테스트는 "결과는 시작 3시간 뒤 공개" 같은 가정 위에 있고, 실제 수집 지연이나 PC가 꺼져 있던 시간은 재현하지 못합니다. 운영 성능은 그 시점에 실제로 남긴 예측으로만 말할 수 있습니다. 그래서 스냅샷을 찍는 순간 그때까지 받은 자료로 다시 학습해 예측을 같이 저장하고, 매주 채점합니다.

**Q. 나중에 예측을 조작하거나 채워 넣을 수 없다는 걸 어떻게 보장하나요?**
세 겹입니다. 예측은 스냅샷을 찍는 같은 실행에서만 만들고, DB 외래 키가 예측의 기준 시각이 스냅샷과 정확히 같은지 확인하며, 기록 시각은 기준 시각 뒤 10분 안이어야 합니다. 그리고 두 테이블 모두 수정·삭제가 막혀 있습니다.

**Q. 검증도 안 된 모델 확률을 왜 저장하나요?**

**Q. 활성 모델은 어떻게 정하나요?**
3b 시험 실행 중 검증 시즌 로그 손실이 가장 낮은 모델입니다. 시험 성적은 Dixon-Coles가 아주 조금 좋았지만, 시험 성적으로 고르면 시험 구간이 선택에 쓰인 셈이 됩니다. 그래서 Poisson입니다. 모델 코드가 바뀌면 평가받지 않은 코드라서 확률을 만들지 않습니다.

**Q. 예측 단계가 실패하면 스냅샷까지 잃지 않나요?**
예측은 저장점(savepoint) 안에서 실행해서, 실패하면 예측만 취소되고 입력 스냅샷은 남습니다. 실패도 `PREDICTION_FAILED:<오류 종류>`로 기록합니다. 모델 모듈 자체를 불러올 수 없을 때(예: numpy 없는 이미지)도 스냅샷은 계속 찍힙니다. 실제로 이 상황을 피하려고 이미지를 먼저 바꾸고 코드를 나중에 배포했습니다.

## 한계

- **아직 실제 기록이 0건입니다.** 첫 기록은 10/22 19:00 UTC이고, PC와 Docker가 켜져 있어야 쌓입니다. 꺼져 있던 시점은 MISSED로 남습니다.
- **의미 있는 판단에는 오래 걸립니다.** 한 시즌이 380경기라, 300경기 이상 쌓이려면 거의 한 시즌이 걸립니다.
- **활성 모델 교체는 자동이 아닙니다.** 3b 백테스트와 3c 평가를 다시 돌려야 바뀝니다(설계 원칙: 새 모델은 평가를 통과한 뒤 활성화).
- **알려진 약점이 그대로입니다.** 승격 팀을 과대평가하고(예: 코번트리 홈 승 0.375), 강팀 확률을 눌러 냅니다(3c).
- **Markdown 보고서는 명령줄에서 만듭니다.** Airflow는 DB에만 저장하고, 휴대폰 알림은 4단계 계획입니다.
