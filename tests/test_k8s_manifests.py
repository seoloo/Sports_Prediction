"""8a 쿠버네티스 정의 검사(클러스터 없이). kubectl kustomize로 그려 본 결과에 운영 규칙이 들어 있는지 본다."""
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def rendered() -> list[str]:
    if shutil.which("kubectl") is None:
        pytest.skip("kubectl 없음")
    output = subprocess.run(["kubectl", "kustomize", str(ROOT / "infra" / "k8s")], capture_output=True, text=True,
                            encoding="utf-8", check=True).stdout
    return output.split("\n---\n")


def documents(rendered, kind):
    return [doc for doc in rendered if f"\nkind: {kind}\n" in f"\n{doc}\n"]


def test_both_services_run_exactly_once_and_safely(rendered):
    deployments = documents(rendered, "Deployment")
    assert len(deployments) == 2
    for doc in deployments:
        # 수집기는 API 한도 때문에 하나만: 교체할 때도 옛 파드를 먼저 내린다
        assert "replicas: 1" in doc and "type: Recreate" in doc
        assert "runAsNonRoot: true" in doc and "readOnlyRootFilesystem: true" in doc
        assert "allowPrivilegeEscalation: false" in doc and "- ALL" in doc
        assert "imagePullPolicy: Never" in doc and "limits:" in doc
        assert "name: pipeline-secrets" in doc   # 비밀 값은 Secret 참조로만


def test_no_secret_values_in_the_repository_manifests(rendered):
    assert not documents(rendered, "Secret")
    text = "\n".join(rendered)
    for key in ("POSTGRES_PASSWORD", "S3_SECRET_KEY", "FOOTBALL_DATA_ORG_API_KEY"):
        assert key not in text


def test_pods_use_the_same_host_names_as_compose(rendered):
    services = {line.split(": ")[1] for doc in documents(rendered, "Service") for line in doc.splitlines()
                if line.startswith("  name: ")}
    assert services == {"postgres", "kafka", "object-store"}
    config = documents(rendered, "ConfigMap")[0]
    assert "POSTGRES_HOST: postgres" in config and "KAFKA_BOOTSTRAP: kafka:9092" in config
    assert "S3_ENDPOINT_URL: http://object-store:8333" in config
