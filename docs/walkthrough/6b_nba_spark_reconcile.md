# 6b. NBA 순위표를 Spark로 다시 계산해 dbt와 대조

작성: 2026-10-23. 상태: **완료.**

## 한 줄 요약

4a에서 축구 시점 특징을 dbt와 Spark로 따로 계산해 맞춰 봤던 것을 **NBA 순위표**에도 붙였습니다. dbt는 팀별로 펼친 Gold(`nba_team_game_results`)를 모아 순위를 만들고, Spark는 **끝난 경기 원자료에서 처음부터** 펼치고·합치고·순위를 매깁니다. 순위 규칙(승률 → 득실 차 → 팀 이름)까지 두 엔진이 따로 구현한 결과, **270행(시즌 × 팀) × 9열 불일치 0**이었습니다.

## 흐름

```text
dbt build ─→ staging.stg_games_known ─→ gold.nba_team_game_results ─→ gold.nba_standings (정답)
                     │                                                         │
      export: nba_games(끝난 경기 원자료)                       export: dbt_nba_standings
                     │                                                         │
                     ▼                                                         │
   Spark compute_nba_standings: 펼치기 → groupBy → row_number(창 함수)          │
                     │                                                         │
                     └──────────── reconcile_nba: (시즌, 팀)마다 9열 비교 ←─────┘
```

같은 스냅샷(내용 해시 ID) 안의 파일끼리만 비교하므로, 내보낸 뒤 Gold가 다시 만들어져도 비교가 흔들리지 않습니다.

## 바꾼 것

| 변경 | 위치 | 이유 |
|---|---|---|
| NBA 내보내기 | `pipelines/lake/export.py`의 `nba_games`, `dbt_nba_standings` | Spark 입력은 Gold가 아니라 원자료(dbt의 중간 단계를 거치지 않아야 독립 구현) |
| Spark 순위표 | `spark_features.compute_nba_standings` | `unionByName`으로 홈·원정 펼치기, `groupBy`로 합계, `row_number`로 순위 |
| 대조 | `reconcile.reconcile_nba` | 경기 수·승·패·득점·실점·득실 차는 정확히, 승률은 넷째 자리, 평균 득실 차는 둘째 자리 반올림 오차까지 |
| 통과 조건 | `reconcile.reconcile` | 축구와 NBA 둘 다 통과해야 `passed`. NBA 자료가 없는 환경(테스트·CI)은 NBA를 통과로 본다 |

매일 DAG의 `spark_reconcile` 작업이 이 대조를 함께 돌리므로, NBA가 어긋나면 그날 작업이 실패하고 텔레그램으로 알립니다.

## 사용한 기술

| 기술 | 어디에 | 어떻게 썼나 |
|---|---|---|
| PySpark DataFrame(`unionByName`, `groupBy`, `count(when)`) | `compute_nba_standings` | 홈·원정 2행 펼치기, 승·패 수 세기 |
| Spark 창 함수(`Window.partitionBy().orderBy()`, `row_number`) | 같은 곳 | 시즌마다 승률 → 득실 차 → 팀 이름 순위. dbt의 `row_number() over (...)`와 같은 규칙을 다른 엔진으로 |
| Parquet + Hive 파티션 경로 | 내보내기 | `season=…` 경로. dbt 파일은 시즌이 경로에, Spark 결과는 파일 안 열에 있어 대조 코드가 두 경우를 모두 읽는다 |
| pyarrow로 대조 | `reconcile_nba` | 검사하는 쪽이 검사받는 엔진(Spark)에 기대지 않게 |
| 내용 해시 스냅샷 | `export.snapshot_id` | 같은 Gold면 같은 스냅샷, Spark도 다시 계산하지 않음(멱등) |

## 확인 결과(2026-10-23)

| 확인 | 결과 |
|---|---|
| 실제 데이터(Spark 4.1.3 컨테이너) | 스냅샷 `1d01414f29fc689e`: NBA 끝난 경기 10,245 → 순위표 270행 × 9열 불일치 0. 축구 특징 12,264경기 × 18열 불일치 0 |
| Spark 단위 테스트 | 승률 → 득실 차 → 이름 순위와 포스트시즌 제외를 Python 독립 계산과 비교 |
| 대조 테스트 | dbt 결과를 Spark 자리에 두면 통과, 두 팀 순위를 뒤바꾸면 그 두 칸만 불일치 |

## 면접에서 나올 만한 질문

**Q. 같은 결과를 두 번 계산하는 건 낭비 아닌가요?**
두 번 계산하는 목적은 결과가 아니라 **규칙을 검증하는 것**입니다. 순위표처럼 단순해 보이는 것도 동률 처리, 포스트시즌 제외, 반올림 같은 결정이 들어갑니다. 한 엔진에서만 만들면 그 결정이 틀려도 드러나지 않습니다. 다른 엔진·다른 경로로 같은 규칙을 구현해 칸 단위로 맞추면, 어느 쪽이든 실수가 있을 때 바로 보입니다. 자료가 작아 비용도 몇 초입니다.

**Q. 왜 Spark는 Gold가 아니라 원자료에서 시작하나요?**
dbt의 중간 테이블(`nba_team_game_results`)을 입력으로 쓰면, 그 중간 단계에 있는 실수(예: 팀 행을 잘못 펼침)를 Spark도 그대로 물려받아 대조가 통과해 버립니다. 원자료에서 따로 시작해야 독립된 검증이 됩니다.

## 한계

- **순위 규칙은 단순화한 것입니다.** 실제 NBA의 동률 규칙과 컨퍼런스 구분은 두 구현 모두 없습니다. 같은 단순 규칙을 두 엔진이 같게 구현했다는 것까지 확인했습니다.
- **Spark는 단일 컨테이너 local 모드입니다.** 자료가 작아 클러스터가 필요 없습니다.
