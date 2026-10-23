# 메달리온 아키텍처 (Bronze · Silver · Gold)

작성: 2026-10-21(3단계 완료 시점). 숫자는 이 날의 로컬 데이터입니다. 행 수는 5a(다섯 리그) 이후 값입니다.

## 한 줄 요약

받은 원본을 그대로 두는 **Bronze**, 정제해서 버전으로 쌓는 **Silver**, 분석·모델이 바로 쓰는 **Gold**로 데이터를 세 층으로 나눴습니다. 층마다 규칙이 다르고, 아래 층에서 위 층을 언제든 다시 만들 수 있습니다.

```text
           수집기(배치·스트리밍)                정제·버전                       변환(dbt)
외부 소스 ───────────────→  BRONZE  ──────────────→  SILVER  ──────────────→  GOLD  ──→ 모델·화면
            원본 그대로          (S3, 내용 해시)      (Postgres, 버전·제약)      (Postgres 테이블·뷰)
                                     │                      │
                                ingest.raw_files ── raw_file_id로 연결(계보)
```

## 왜 층을 나누나

| 이유 | 이 프로젝트에서 |
|---|---|
| 다시 처리할 수 있어야 한다 | 정규화 규칙에 버그가 있어도, Bronze 원본이 있으면 Silver를 다시 만들 수 있다 |
| 문제를 층마다 다르게 다룬다 | Bronze는 "받은 그대로 보존", Silver는 "형식·정합성 보장", Gold는 "쓰기 편한 모양" |
| 책임이 분리된다 | 수집기는 Bronze·Silver까지만, 분석 규칙(순위·특징·소스 우선순위)은 Gold(dbt)에만 둔다 |
| 계보를 추적할 수 있다 | Silver의 모든 버전이 어느 원본 파일에서 왔는지(`raw_file_id`) 남는다 |

## Bronze: 받은 원본 그대로

**저장소:** S3 호환 객체 저장소(로컬은 SeaweedFS), 버킷 `bronze`
**경로 규칙:** `소스/데이터셋/시즌/sha256=내용해시.확장자`
예: `football-data-co-uk/E0/2021-22/sha256=335afcba….csv`

| 소스 | 객체 수 | 크기 | 수집 방식 |
|---|---|---|---|
| openfootball (`openfootball/en.1`) | 6 | 0.62MB | 자동(CC0) |
| football-data.co.uk (`football-data-co-uk/E0`) | 6 | 0.96MB | 사용자가 직접 내려받은 파일만(약관) |
| football-data.org (`football-data-org/PL`) | 1 | 0.37MB | API, 내용이 바뀔 때만 새 객체 |

가공한 Parquet는 Bronze가 아니라 별도 `lake` 버킷에 둡니다(4a). Gold를 내보낸 것과 Spark 결과라서, 받은 원본만 두는 Bronze 규칙과 섞지 않습니다.

**규칙**

- 원본을 절대 고치지 않는다. 파싱도 하지 않고 바이트 그대로 둔다.
- 경로에 내용 해시가 들어간다(content-addressed). 같은 내용은 같은 경로라 여러 번 받아도 한 번만 저장된다(멱등).
- 원본 목록은 Postgres `ingest` 스키마에 둔다.

| 테이블 | 행 | 내용 |
|---|---|---|
| `ingest.sources` | 4 | 소스별 사용 조건 검토 결과(자동 수집 허용 여부 포함) |
| `ingest.raw_files` | 47 | 원본 파일 목록(해시, 크기, 객체 경로, 처음 받은 시각) |
| `ingest.fetches` | 63 | 가져올 때마다의 기록(같은 내용을 다시 받았는지) |
| `ingest.runs` | 63 | 실행 기록(성공·실패, 통계, 오류) |

## Silver: 정제하고 버전으로 쌓기

**저장소:** PostgreSQL 16, `silver` 스키마

| 테이블 | 행 | 내용 |
|---|---|---|
| `silver.teams` | 182 | 기준 팀(축구 152: openfootball 이름, 농구 30: balldontlie 이름, `sport` 열로 구분) |
| `silver.team_aliases` | 277 | 소스별 팀 이름·ID → 기준 팀(검토한 대조표) |
| `silver.matches` | 12,264 | 경기의 변하지 않는 정체성(리그·시즌·홈·원정) |
| `silver.match_revisions` | 12,264 | openfootball 경기 내용(일정·점수)의 버전 |
| `silver.live_match_revisions` | 1,752 | football-data.org 경기 상태·점수의 버전(published_at = API 갱신 시각) |
| `silver.match_stats_revisions` | 1,969 | 경기 기록(슈팅·유효 슈팅·퇴장)의 버전(co.uk, 3g) |
| `silver.odds_revisions` | 166,589 | 배당의 버전(북메이커 × 시장 × 선택지 × 시점) |
| `silver.games` | 11,429 | NBA 경기의 정체성(소스 경기 ID). 같은 대진이 여러 번 열려 축구 `matches`와 분리(5d) |
| `silver.game_odds_revisions` | 712 | NBA 승패 배당의 버전(북메이커 × HOME·AWAY, 5d-b) |
| `silver.game_revisions` | 11,431 | NBA 일정·상태·점수의 버전. 무승부 없음을 CHECK로 강제 |
| `silver.quality_issues` | 2 | 수집 단계에서 걸러낸 레코드와 사유 |
| `silver.stream_rejections` | 0 | 스트리밍에서 반영하지 않은 이벤트와 사유 |

**규칙**

- **정체성과 내용을 나눈다.** `matches`는 "어떤 경기인가"만, 바뀔 수 있는 일정·점수는 `*_revisions`에 버전으로. 정체성의 키는 종목 규칙에 맞춘다: 축구 리그는 (리그, 시즌, 홈, 원정)이 한 번뿐이지만 NBA는 같은 대진이 여러 번 열려 소스 경기 ID를 쓴다(`silver.games`).
- **덮어쓰지 않는다.** 내용이 바뀐 경우에만 다음 버전을 추가하고, 수정·삭제는 트리거가 막는다.
- **시점을 기록한다.** 모든 버전에 `published_at`(출처 발표 시각, 모르면 NULL)과 `received_at`(받은 시각)이 있다.
- **기준 시각 함수로 읽는다.** `silver.match_revisions_known_at(as_of)` 등은 그 시점에 알려진 최신 버전만 돌려준다.
- **형식은 DB가 보장한다.** 점수 0 이상, 홈·원정 점수는 함께 있거나 함께 없음, 배당 > 1 같은 CHECK 제약.
- **추측하지 않는다.** 결측은 NULL, 팀 연결은 대조표로만, 소스끼리 다르면 덮지 않고 기록.
- **계보.** 각 버전이 `raw_file_id`로 Bronze 원본을 가리킨다.



알림 발송 기록(`notify.sent_messages`, 4b)은 데이터 층이 아니라 운영 기록입니다. 같은 알림을 두 번 보내지 않기 위한 장부라 본문 없이 해시만 남깁니다.

모델 백테스트 기록(`modeling` 스키마, 3b)도 같은 성격입니다. Gold 데이터셋을 읽어 만든 결과지만, "그 모델 버전이 그 데이터로 낸 예측"이라는 증거라 수정할 수 없게 막았습니다.

| 테이블 | 행 | 내용 |
|---|---|---|
| `modeling.backtest_runs` | 499 | 실행 기록(모델 버전, 하이퍼파라미터, 검증/시험, 평가 시즌, 데이터셋 해시, 요약 성적) |
| `modeling.backtest_predictions` | 364,572 | 예측마다 학습 기준 시각, 학습 경기 수, 확률, 결과 |
| `modeling.prospective_reports` | 1 | 경기 전 예측의 주간 보고서(3d). 같은 주라도 결과 정정으로 내용이 바뀌면 새 행 |
| `modeling.game_backtest_predictions` | 242,212 | NBA 백테스트 예측(홈 승 확률 하나, 5d-c). 실행 기록은 `backtest_runs`의 `competition_code = 'nba'` |
| `modeling.evaluation_reports` | 9 | 평가 보고서(어떤 실행을 어떤 방법·기준으로 평가했는지와 결과, 3c). 방법 버전 3c.1·3c.2·3c.3(3e) |

## Gold: 바로 쓰는 모양

**저장소:** PostgreSQL 16, `staging`(뷰)·`gold`(테이블) 스키마. **dbt**가 SQL로 만들고 품질 테스트를 함께 돌린다.

| 모델 | 종류 | 행 | 내용 |
|---|---|---|---|
| `staging.stg_matches_known` | 뷰 | 2,280 | 기준 시각에 알려진 경기 정보. football-data.org 우선, 없으면 openfootball(`fixture_source` 열) |
| `staging.stg_odds_known` | 뷰 | 166,589 | 기준 시각에 알려진 배당 |
| `gold.team_match_results` | 테이블 | 21,558 | 끝난 경기를 팀 관점 2행으로 |
| `gold.league_standings` | 테이블 | 672 | 리그·시즌별 순위표 |
| `gold.match_odds_1x2` | 테이블 | 33,021 | 1X2 배당과 마진을 뺀 확률(시장 기준선) |
| `gold.match_stats` | 테이블 | 1,969 | 끝난 경기의 기록(3g). 대상 경기보다 앞서 끝난 경기의 것만 모델 입력 |
| `gold.match_features_pit` | 테이블 | 11,416 | 시간순 학습 데이터셋(시작 전 정보만으로 만든 팀 지표, 3a) |
| `staging.stg_games_known` | 뷰 | 11,429 | 기준 시각에 알려진 NBA 경기 정보(6a) |
| `gold.nba_team_game_results` | 테이블 | 20,490 | 끝난 NBA 경기를 팀 관점 2행으로(무승부 없음) |
| `gold.nba_standings` | 테이블 | 270 | 시즌별 정규 시즌 순위(승률 → 득실 차). 소스가 플레이인·NBA컵 결승을 정규 시즌으로 표시한 시즌은 일부 팀이 83경기 |
| `gold.nba_game_odds_h2h` | 테이블 | 358 | NBA 승패 배당과 마진을 뺀 확률 |

**규칙**

- 언제든 처음부터 다시 만든다. `dbt build`를 다시 돌려도 결과가 같다(체크섬으로 확인).
- 분석 규칙은 여기에만 둔다. 소스 우선순위, 순위 동률 처리, 특징 계산 창 같은 결정.
- 시점 선택은 Silver의 `*_known_at` 함수에 맡긴다. 같은 규칙을 두 곳에 두지 않는다.
- dbt 변수 `as_of`로 과거 시점 기준으로 다시 계산할 수 있다. 예: `--vars "{as_of: '2025-01-01T00:00:00Z'}"`
- 품질 테스트 38개(유일성, 필수값, 허용값, 순위표 정합성, 확률 합 = 1, 시점 누수, NBA 무승부 없음)가 실패하면 빌드가 실패한다. dbt 출력의 `PASS=50`은 모델 12개 + 테스트 38개를 합친 수다(3g에서 경기 기록 모델 2개·테스트 4개, 6a에서 NBA 모델 4개·테스트 12개 추가). 6a부터 수집기별 신선도(마지막 수집 시각)도 `dbt source freshness`로 검사한다.

## 데이터 하나가 층을 지나는 예

football-data.org가 한 경기의 점수를 2:0으로 갱신한 경우:

1. **Bronze:** 수집기가 전체 경기 목록(380경기)을 받아 `football-data-org/PL/2026-27/sha256=….json`에 저장하고 `ingest.raw_files`에 기록.
2. **Kafka:** 응답 내용이 바뀌었으므로 경기별 이벤트를 보냄.
3. **Silver:** 소비자가 그 경기의 직전 버전과 비교해 내용이 바뀐 경우에만 `live_match_revisions`에 다음 버전을 추가(published_at = API 갱신 시각).
4. **Gold:** 다음 dbt 실행에서 `stg_matches_known`이 이 버전을 우선 쓰고, 순위표·학습 데이터셋이 다시 만들어짐.
5. **나중에 물어보기:** "갱신 전 시점의 순위표"는 `as_of`를 그 시점으로 주고 다시 만들면 나온다.

## 교과서적인 메달리온과 다른 점

| 보통 | 이 프로젝트 | 이유 |
|---|---|---|
| 세 층 모두 데이터 레이크(Parquet, Delta·Iceberg) | Bronze만 객체 저장소, Silver·Gold는 Postgres | 데이터가 수십만 행 규모라 DB로 충분하고, 트리거·CHECK·트랜잭션으로 이력 보존 규칙을 강하게 걸 수 있다 |
| Silver는 "정제된 최신 상태" | Silver는 "정제된 모든 버전" | 시점별로 그때의 상태를 재현해야 해서(누수 방지) 최신만 두면 안 된다 |
| Gold는 집계 테이블 | 집계 + 시점 기준 재계산 가능 | 백테스트와 운영 평가가 "그 시점" 기준이어야 해서 |

4a에서 Gold를 Parquet(`lake` 버킷, 시즌 파티션)로 내보내고 Spark로 같은 특징을 다시 계산해 dbt 결과와 대조했습니다(2,660경기 × 18열 불일치 0). 데이터가 커지면 Gold 계산을 이 경로로 옮길 수 있습니다.

## 면접에서 나올 만한 질문

**Q. 메달리온 아키텍처를 왜 썼나요?**
재처리와 책임 분리 때문입니다. Bronze에 원본을 그대로 두니 정규화 규칙을 고쳐도 Silver를 다시 만들 수 있고, 분석 규칙은 Gold의 dbt에만 둬서 수집기를 건드리지 않고 바꿀 수 있습니다. 실제로 Gold의 소스 우선순위를 나중에 바꿨을 때 수집기는 손대지 않았습니다.

**Q. Bronze에 내용 해시 경로를 쓴 이유는요?**
같은 파일을 여러 번 받아도 한 번만 저장되게(멱등) 하기 위해서입니다. football-data.org는 6시간마다 조회하는데, 응답이 같으면 경로도 같아 새로 저장하지 않습니다. 또 DB 기록이 실패해도 다음 실행이 같은 객체를 그대로 다시 씁니다.

**Q. Silver에 왜 최신 값만 두지 않나요?**
"그 시점에 무엇을 알았는지"를 재현해야 하기 때문입니다. 점수 정정, 일정 변경, 배당 변화가 있을 때 최신 값만 있으면, 과거 시점 예측을 평가할 때 미래 정보가 섞입니다. 모든 버전과 발표·수신 시각을 두고, 기준 시각 함수로 그 시점의 버전만 꺼냅니다.

**Q. Gold가 틀렸으면 어떻게 고치나요?**
Gold는 언제든 다시 만드는 층이라, dbt 모델을 고치고 다시 빌드하면 됩니다. Silver와 Bronze는 그대로입니다. 반대로 Silver에 버그가 있으면 Bronze 원본으로 Silver를 다시 만들 수 있습니다.
