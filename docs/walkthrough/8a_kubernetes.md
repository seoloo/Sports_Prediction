# 8a. 수집기·소비자를 Kubernetes에 배포

작성: 2026-10-24. 상태: **완료.** 8단계(상시 운영)의 첫 단계입니다.

## 한 줄 요약

상시로 도는 두 서비스(football-data.org 수집기, Kafka 소비자)를 **Kubernetes 배포 정의(Kustomize)**로 만들고, 로컬 **k3s**(컨테이너 하나짜리 경량 Kubernetes)에 올렸습니다. 실제 데이터로 아래를 확인한 뒤 클러스터를 지웠습니다.
- 수집기 파드가 API를 조회해 실행 기록을 남겼습니다.
- 소비자 파드가 Kafka 소비자 그룹에 들어가 3개 파티션을 모두 받았습니다(밀린 메시지 0).
- 파드를 지우면 Kubernetes가 새 파드를 다시 띄웠습니다.

**클러스터는 확인할 때만 띄웁니다.** 같은 PC에서 Kubernetes를 상시로 돌려도 "PC가 꺼져도 도는 운영"은 되지 않고 메모리만 더 쓰기 때문입니다(이번 측정 약 600MB). 상시 운영은 미니 PC가 생기면 이 정의를 그대로 올립니다.

## 구성

```text
PC(Docker)
├─ compose: Postgres · Kafka · SeaweedFS · Airflow (그대로)
└─ k3s 컨테이너(sports-prediction-k3s, compose 네트워크에 붙음, API는 127.0.0.1:46443)
     namespace sports-prediction
     ├─ Deployment fdorg-poller     (replicas 1, Recreate)
     ├─ Deployment stream-consumer  (replicas 1, Recreate)
     ├─ ConfigMap pipeline-config   (접속 이름: postgres, kafka:9092, object-store:8333)
     ├─ Secret pipeline-secrets     (.env에서 필요한 5개 키만, 저장소에 없음)
     └─ Service postgres / kafka / object-store (selector 없음) + EndpointSlice → compose 컨테이너 주소
```

파드 안의 접속 이름은 compose와 같습니다. 그래서 코드·설정을 바꾸지 않습니다. Kafka는 브로커가 `kafka:9092`를 광고(advertised listener)하므로, Service 이름도 반드시 `kafka`여야 클라이언트가 두 번째 연결부터도 닿습니다.

## 바꾼 것

| 변경 | 위치 | 이유 |
|---|---|---|
| 코드를 구운 이미지 | `infra/k8s/Dockerfile` | compose는 코드를 볼륨으로 연결하지만, Kubernetes에서는 이미지 하나가 배포 단위. 의존성 파일은 compose 이미지와 같음 |
| 배포 정의 | `infra/k8s/`(`kustomization.yaml`, Deployment 2, Service 3, Namespace) | kubectl에 든 Kustomize로 끝나서 Helm 실행 파일이 필요 없음. ConfigMap 이름에 내용 해시가 붙어 설정이 바뀌면 파드가 다시 뜸 |
| 로컬 확인 스크립트 | `scripts/k8s_local.py`(`up`·`status`·`down`) | compose 수집기·소비자 멈춤 → k3s 시작 → 이미지 넣기 → Secret·EndpointSlice → 배포. `down`은 클러스터 삭제 후 compose 다시 시작 |
| 정의 검사 테스트 | `tests/test_k8s_manifests.py` | 클러스터 없이 그려 본 결과로 운영 규칙 확인 |

## 운영 규칙과 이유

| 규칙 | 어디에 | 이유 |
|---|---|---|
| replicas 1 + `Recreate` | 두 Deployment | 수집기가 둘이 되면 API 한도(분당 10회)를 나눠 쓰다 넘는다. 교체할 때도 옛 파드를 먼저 내린다 |
| 루트 아님, 읽기 전용 루트 파일시스템, 권한 상승 금지, 모든 capability 제거, seccomp 기본값 | `securityContext` | 쓰기가 필요한 곳은 `/tmp`(emptyDir)뿐 |
| 메모리 요청 64Mi / 상한 256Mi | `resources` | compose에서 잰 사용량(각 35~70MB)에 여유. 새는 경우 다른 서비스를 밀어내지 않고 이 파드만 다시 시작 |
| 비밀 값은 저장소 밖 | `pipeline-secrets` | 스크립트가 .env에서 5개 키만 골라 kubectl 표준 입력으로 만든다(파일·출력에 남기지 않음) |
| 레지스트리 없이 이미지 넣기 | `imagePullPolicy: Never` + `ctr images import` | 로컬 확인에 레지스트리를 따로 두지 않음 |
| 확인 중에는 한쪽만 | 스크립트가 compose 쪽을 멈추고 되살림 | 같은 소비자 그룹·같은 API 키를 두 곳이 동시에 쓰지 않게 |

## 사용한 기술

| 기술 | 어디에 | 어떻게 썼나 |
|---|---|---|
| Kubernetes Deployment·ConfigMap·Secret·Service | `infra/k8s/` | 상시 서비스 두 개를 선언형으로 |
| Kustomize(`kubectl kustomize`/`apply -k`) | `kustomization.yaml` | 공통 이름공간·라벨, ConfigMap 생성(내용 해시 이름) |
| selector 없는 Service + EndpointSlice | `external-services.yaml` + 스크립트 | 클러스터 밖 저장소를 클러스터 안 이름으로(미니 PC로 옮기면 주소만 바꿈) |
| k3s(`rancher/k3s:v1.36.5-k3s1`, 89MB) | 스크립트 | traefik·servicelb·local-storage·metrics-server를 꺼서 가볍게. 승인받고 받은 이미지 |
| Pod securityContext | 두 Deployment | 최소 권한 실행 |

## 확인 결과(2026-10-24)

| 확인 | 결과 |
|---|---|
| 배포 | 두 Deployment 모두 롤아웃 완료, 파드 Running |
| 수집기 파드 | PL·BL1·PD·SA 조회, `ingest.runs`에 실행 기록(새 내용 없음 → 이벤트 0), API 남은 요청 수 확인 |
| 소비자 파드 | 소비자 그룹 `silver-match-writer` STABLE, 멤버 1개(k3s 노드 주소 172.19.0.5)가 파티션 0·1·2를 받음, 밀린 메시지 0 |
| 이름 풀이 | 파드 안에서 kafka·postgres·object-store → 클러스터 IP |
| 자가 회복 | 수집기 파드를 지우자 새 파드가 Ready |
| 메모리 | k3s 컨테이너(클러스터 + 파드 2개) 약 600MB |
| 정리 | 클러스터 삭제, compose 수집기·소비자 다시 실행 |
| 테스트 | 정의 검사 3개(하나만 실행·보안 설정, 저장소에 비밀 값 없음, 접속 이름이 compose와 같음) |

## 면접에서 나올 만한 질문

**Q. 데이터베이스와 Kafka는 왜 Kubernetes에 안 올렸나요?**
상태를 가진 서비스(Postgres, Kafka, 객체 저장소)를 Kubernetes에 올리려면 영구 볼륨, StatefulSet, 백업·업그레이드 절차가 함께 필요합니다. 한 대짜리 환경에서는 얻는 것보다 운영 부담이 큽니다. 실무에서도 관리형 DB·Kafka를 쓰고 앱만 Kubernetes에 올리는 경우가 많습니다. 그래서 상태 없는 앱만 올리고, 저장소는 selector 없는 Service로 연결했습니다. 나중에 저장소를 옮겨도 앱 정의는 그대로입니다.

**Q. Helm을 쓰지 않은 이유는?**
환경별로 바뀌는 값이 적고(이미지 태그, 저장소 주소), Kustomize가 kubectl에 들어 있어 도구를 하나 덜 씁니다. 환경이 여러 개로 늘고 값이 많아지면 Helm 차트나 Kustomize 오버레이로 나눕니다.

**Q. 왜 레플리카를 늘리지 않나요?**
수집기는 API 한도를 지키는 상태(최근 60초 요청 수)를 프로세스 안에 갖고 있어서, 둘이 되면 한도를 넘습니다. 늘리려면 한도를 공유 저장소(예: Redis)로 옮겨야 합니다. 소비자는 늘릴 수 있지만(파티션 3개), 지금 양에는 하나로 충분합니다.

## 한계

- **PC가 꺼지면 함께 꺼집니다.** 상시 운영은 미니 PC로 옮긴 뒤입니다(8b).
- **처음 뜰 때 이름 풀이 오류가 몇 줄 나옵니다.** k3s의 CoreDNS가 파드와 동시에 뜨면서 Kafka 클라이언트가 몇 초간 `kafka`를 못 찾았습니다. 클라이언트가 스스로 다시 시도해 붙었습니다.
- **EndpointSlice 주소는 띄울 때의 compose 컨테이너 주소입니다.** compose 컨테이너를 다시 만들면 주소가 바뀔 수 있어, 그때는 `up`을 다시 실행합니다.
- **이미지는 레지스트리 없이 로컬에서 넣습니다.** 미니 PC나 여러 노드에서는 레지스트리가 필요합니다.
