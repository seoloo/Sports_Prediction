"""로컬 쿠버네티스(k3s 컨테이너 하나)에 수집기·소비자를 올려 확인하고 내린다(로드맵 8a).

평소에는 compose가 수집기·소비자를 돌린다. 이 스크립트는 "쿠버네티스에서도 같은 것이 돈다"를 확인할 때만 쓴다.
클러스터는 확인하는 동안만 띄우고 지운다(메모리 약 0.5~1GB를 쓰므로).

  python scripts/k8s_local.py up       compose 수집기·소비자 멈춤 → k3s 시작 → 이미지 넣기 → 비밀 값·외부 주소 → 배포
  python scripts/k8s_local.py status   파드 상태와 최근 로그
  python scripts/k8s_local.py down     k3s 컨테이너 삭제 → compose 수집기·소비자 다시 시작

- k3s는 compose 네트워크(sports-prediction_default)에 붙어서 Postgres·Kafka·SeaweedFS에 컨테이너 주소로 닿는다.
  파드 안에서는 external-services.yaml의 Service 이름(postgres, kafka, object-store)을 쓰고, 그 주소는 여기서
  EndpointSlice로 넣는다.
- 비밀 값은 .env에서 필요한 키만 골라 kubectl 표준 입력으로 넘긴다(파일·명령줄·출력에 남기지 않음).
- API 서버는 127.0.0.1에만 연다. kubeconfig는 data/k8s/(Git 제외)에 둔다.
"""
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from pipelines.config import ENV_PATH, read_env_file  # noqa: E402

K3S_IMAGE = "rancher/k3s:v1.36.5-k3s1"   # 2026-09-30 배포, kubectl 1.36과 같은 판
CONTAINER = "sports-prediction-k3s"
NETWORK = "sports-prediction_default"
API_PORT = 46443
APP_IMAGE = "sports-prediction-pipeline-app:0.1"
NAMESPACE = "sports-prediction"
KUBECONFIG = ROOT / "data" / "k8s" / "kubeconfig"
COMPOSE = ["docker", "compose", "--env-file", str(ENV_PATH), "-f", str(ROOT / "infra" / "docker-compose.yml"),
           "--profile", "streaming"]
EXTERNAL = {"postgres": ("sports-prediction-postgres-1", 5432), "kafka": ("sports-prediction-kafka-1", 9092),
            "object-store": ("sports-prediction-object-store-1", 8333)}
SECRET_KEYS = ("POSTGRES_USER", "POSTGRES_PASSWORD", "S3_ACCESS_KEY", "S3_SECRET_KEY", "FOOTBALL_DATA_ORG_API_KEY")


def run(command, *, input_text=None, capture=False, check=True) -> str:
    result = subprocess.run(command, input=input_text, text=True, encoding="utf-8", check=check,
                            capture_output=capture)
    return result.stdout if capture else ""


def kubectl(*args, input_text=None, capture=False, check=True) -> str:
    return run(["kubectl", "--kubeconfig", str(KUBECONFIG), *args], input_text=input_text, capture=capture, check=check)


def start_cluster() -> None:
    if run(["docker", "ps", "-aq", "--filter", f"name=^{CONTAINER}$"], capture=True).strip():
        print("이미 있는 k3s 컨테이너를 씁니다.")
    else:
        # traefik(인그레스)·servicelb·local-storage·metrics-server는 쓰지 않아 끈다(메모리 절약).
        run(["docker", "run", "-d", "--name", CONTAINER, "--privileged", "--network", NETWORK,
             "-p", f"127.0.0.1:{API_PORT}:6443", K3S_IMAGE, "server",
             "--disable", "traefik", "--disable", "servicelb", "--disable", "local-storage",
             "--disable", "metrics-server", "--tls-san", "127.0.0.1"])
    for _ in range(60):
        config = run(["docker", "exec", CONTAINER, "cat", "/etc/rancher/k3s/k3s.yaml"], capture=True, check=False)
        if "server:" in config:
            KUBECONFIG.parent.mkdir(parents=True, exist_ok=True)
            KUBECONFIG.write_text(config.replace("https://127.0.0.1:6443", f"https://127.0.0.1:{API_PORT}"),
                                  encoding="utf-8")
            if "Ready" in kubectl("get", "nodes", capture=True, check=False):
                return
        time.sleep(2)
    raise TimeoutError("k3s 노드가 2분 안에 준비되지 않았습니다.")


def load_image() -> None:
    """앱 이미지를 만들고 k3s(containerd)에 직접 넣는다. 레지스트리를 두지 않는다."""
    run(["docker", "build", "-f", str(ROOT / "infra" / "k8s" / "Dockerfile"), "-t", APP_IMAGE, str(ROOT)])
    saver = subprocess.Popen(["docker", "save", APP_IMAGE], stdout=subprocess.PIPE)
    subprocess.run(["docker", "exec", "-i", CONTAINER, "ctr", "images", "import", "-"], stdin=saver.stdout, check=True,
                   stdout=subprocess.DEVNULL)
    if saver.wait() != 0:
        raise RuntimeError("docker save 실패")


def apply_secret_and_endpoints() -> None:
    kubectl("apply", "-f", str(ROOT / "infra" / "k8s" / "namespace.yaml"))
    env = read_env_file(ENV_PATH)
    secret = {"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
              "metadata": {"name": "pipeline-secrets", "namespace": NAMESPACE},
              "stringData": {key: env.get(key, "") for key in SECRET_KEYS}}
    kubectl("apply", "-f", "-", input_text=json.dumps(secret))   # 표준 입력으로만: 디스크·출력에 남기지 않음
    slices = []
    for service, (container, port) in EXTERNAL.items():
        address = run(["docker", "inspect", container, "--format",
                       "{{(index .NetworkSettings.Networks \"" + NETWORK + "\").IPAddress}}"], capture=True).strip()
        slices.append({"apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice", "addressType": "IPv4",
                       "metadata": {"name": f"{service}-compose", "namespace": NAMESPACE,
                                    "labels": {"kubernetes.io/service-name": service}},
                       "ports": [{"name": {"postgres": "postgres", "kafka": "kafka", "object-store": "s3"}[service],
                                  "port": port, "protocol": "TCP"}],
                       "endpoints": [{"addresses": [address]}]})
        print(f"{service} → {container} {address}:{port}")
    kubectl("apply", "-f", "-", input_text=json.dumps({"apiVersion": "v1", "kind": "List", "items": slices}))


def up() -> None:
    print("compose 수집기·소비자를 멈춥니다(API 한도·소비자 그룹을 두 곳이 나눠 쓰지 않게).")
    run([*COMPOSE, "stop", "fdorg-poller", "stream-consumer"])
    start_cluster()
    load_image()
    apply_secret_and_endpoints()
    kubectl("apply", "-k", str(ROOT / "infra" / "k8s"))
    for name in ("fdorg-poller", "stream-consumer"):
        kubectl("-n", NAMESPACE, "rollout", "status", f"deployment/{name}", "--timeout=180s")
    status()


def status() -> None:
    kubectl("-n", NAMESPACE, "get", "pods", "-o", "wide")
    for name in ("fdorg-poller", "stream-consumer"):
        print(f"--- {name} 최근 로그")
        print(kubectl("-n", NAMESPACE, "logs", f"deployment/{name}", "--tail", "5", capture=True, check=False))


def down() -> None:
    run(["docker", "rm", "-f", CONTAINER], check=False)
    print("k3s를 지웠습니다. compose 수집기·소비자를 다시 시작합니다.")
    run([*COMPOSE, "start", "fdorg-poller", "stream-consumer"])


if __name__ == "__main__":
    {"up": up, "status": status, "down": down}[sys.argv[1] if len(sys.argv) > 1 else "status"]()
