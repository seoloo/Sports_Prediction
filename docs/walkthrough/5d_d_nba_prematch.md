# 5d-d. NBA 경기 전 기록·알림

작성: 2026-10-22. 상태: **완료.** 첫 실제 촬영은 2026-10-22 20:00 UTC(한국 23일 05:00) 필라델피아–클리블랜드 3시간 전 시점입니다(Airflow가 자동으로).

## 한 줄 요약

축구에서 쓰던 "경기 전 스냅샷 → 모델 예측 기록 → 텔레그램"을 NBA에 붙였습니다. 시작 24·3·1시간 전에 그때 알려진 입력(최근 5경기, 연전 여부, 배당)을 얼리고, 같은 순간 활성 모델(점수 차 등급+연전)의 확률을 **평가용으로만** 저장합니다. 모델이 검증 전이라 알림에는 확률을 보내지 않고 보류 사유를 보냅니다. 얼려 둔 배당은 나중에 같은 시점의 시장 확률로 채점할 때 씁니다(5e).

## 바꾼 것

| 변경 | 위치 | 이유 |
|---|---|---|
| NBA 스냅샷·예측 표 | `pipelines/sql/016_game_prematch.sql` | 축구 표는 축구 경기와 3개 확률을 전제로 한다. 같은 규칙(같은 as_of 외래 키, 불변, WITHHELD 강제)을 2지선다로 |
| 스냅샷 실행기 | `pipelines/nba_snapshots.py` | 시점 판단(`decide`)·90분 유예·7일 놓침 기록은 축구 함수를 그대로 쓴다 |
| 경기 전 예측 | `pipelines/modeling/nba_prospective.py` | 활성 모델은 축구와 같은 `active_model`(시험 실행 중 검증 성적 1위). 학습은 그 순간 받아 둔 결과만 |
| 시점 조회 데이터셋 | `nba_backtest.load_games(conn, as_of)` | 백테스트(지금 기준)와 경기 전 예측(스냅샷 시각 기준)이 같은 코드를 쓴다 |
| 시장 기준 확률 | `pipelines/modeling/market.py`의 `nba_market_fair` | Pinnacle → 북메이커 3곳 이상 평균. 마진을 뺀 확률을 채점의 비교 기준으로 |
| 알림 | `pipelines/notify/alerts.py` | 축구와 한 메시지로. 놓친 시점은 건수만, 놓친 것만 있으면 보내지 않음 |
| DAG | `dags/prematch_snapshots.py` | 축구·NBA 스냅샷을 별개 작업으로. 한 종목이 실패해도 다른 종목과 알림은 진행(`all_done`) |

## 흐름

```text
prematch_snapshots DAG(30분마다)
 ├ fetch_due_odds / fetch_due_nba_odds        배당(5b·5d-b)
 ├ take_due_snapshots / take_due_nba_snapshots
 │    NBA: 시작 24·3·1시간 전 → 입력 얼림(game_prematch_inputs, 그때 알려진 배당 포함)
 │         → 예측(game_prematch_predictions, 확률은 평가용, WITHHELD)
 └ notify_snapshots   "🏀 NBA 경기 전 스냅샷 … 찍음 · 예측 보류(모델 미검증)"
```

## 사용한 기술

| 기술 | 어디에 | 어떻게 썼나 |
|---|---|---|
| Postgres 복합 외래 키 | `game_prematch_predictions` → `game_prematch_inputs(game_id, offset_label, as_of)` | 예측은 스냅샷과 같은 기준 시각에만 만들 수 있음(소급 기록 차단) |
| CHECK 제약 | 016 | 표시 상태 READY는 검증 통과 + 확률 있음일 때만, 기록 시각은 as_of 뒤 10분 안 |
| 저장점(`conn.transaction()` 중첩) | `record_for_snapshot` | 예측이 실패해도 입력 스냅샷은 남김 |
| 시점 조회 함수 | `game_revisions_known_at(as_of)`, `game_odds_known_at(as_of)` | 입력·학습·배당 모두 그 순간 알려진 버전만 |
| 함수 재사용 | `snapshots.decide`, `inputs_hash`, `prospective.active_model` | 축구와 같은 규칙을 한곳에서 |
| Airflow `trigger_rule="all_done"` | DAG | 종목별 작업이 서로를 막지 않음 |
| 알림 중복 방지 | `notify.sent_messages`(키 `nba:경기:시점`) | 같은 스냅샷은 한 번만, 축구 키와 겹치지 않음 |

## 2026-10-22 확인 결과

| 확인 | 결과 |
|---|---|
| 활성 모델 | `margin_rating_rest`(ridge 3, 반감기 90일, 연전), 평가 보고서 12, NOT_VALIDATED |
| 첫 실행(10:37 UTC) | 놓침 44건 = 최근 7일에 시작한 14경기 × 3 + 오늘 두 경기의 24시간 전(어제 23:00, 90분 넘음). 찍음 0(오늘 경기의 3시간 전은 20:00 UTC) |
| 같은 시각 재실행(Airflow 컨테이너) | 새 기록 0(멱등), 불러오기 오류 없음 |
| 알림 | 텔레그램 미설정이라 `NOT_CONFIGURED`(설정하면 보냄) |

## 테스트가 확인하는 것

| 테스트 | 확인 내용 |
|---|---|
| `test_market_fair_uses_pinnacle_then_consensus` | 출처 우선순위, 북메이커 2곳뿐이면 기준으로 삼지 않음 |
| `test_missed_only_is_not_sent_alone` | 놓친 시점만 있으면 알림을 보내지 않음 |
| `test_snapshots_record_withheld_predictions_alerts_and_weekly_scoring` | 220경기로 학습한 확률은 저장되지만 WITHHELD(MODEL_NOT_VALIDATED), 그때 알려진 배당이 입력에 얼려짐, 멱등, 1시간 전 놓침, 알림에 확률 없음, 같은 알림 다시 안 보냄, 결과 후 주간 채점(5e) |

## 면접에서 나올 만한 질문

**Q. 모델 확률을 저장은 하면서 왜 보여 주지 않나요?**
검증 기준을 통과하지 못한 확률을 보여 주면 근거 없는 숫자로 판단하게 됩니다. 하지만 저장해 두지 않으면 나중에 "경기 전에 실제로 냈던 확률"로 채점할 수 없습니다. 그래서 확률은 경기 전 그 순간에 평가용으로 남기고, 표시 상태는 DB 제약으로 WITHHELD를 강제합니다. 쌓인 기록과 경기 전 배당으로 시장 비교를 하면 검증 상태가 바뀔 수 있습니다.

**Q. 축구와 NBA 스냅샷을 왜 같은 함수로 안 합쳤나요?**
시점 판단(`decide`), 유예, 놓침 기록, 해시는 같은 함수를 씁니다. 다른 것은 경기 정체성(축구는 리그·시즌·홈·원정, NBA는 소스 경기 ID)과 확률 개수(3개 대 1개)라, 표와 입력 모으기만 종목별로 둡니다. 규칙은 하나, 모양만 종목에 맞춘 것입니다.

## 한계

- **늦은 경기 배당은 몇 시간 전 값입니다**(5d-b 예산).
- **선수 정보가 없습니다.** 경기 직전 결장 소식은 배당에만 반영됩니다.
