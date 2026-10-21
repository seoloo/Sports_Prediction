"""파이프라인 접속 설정.

값은 프로젝트 루트의 .env(Git 제외)에서 읽고, 같은 이름의 환경 변수가 있으면 환경 변수를 우선한다.
.env가 없으면 `python -m pipelines.config init-env`로 무작위 비밀번호가 들어간 .env를 만든다.
비밀번호를 코드·문서·Git에 남기지 않기 위해서다.
"""
import os
import secrets
import sys
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"


def read_env_file(path: Path = ENV_PATH) -> dict:
    """KEY=VALUE 형식의 .env를 dict로 읽는다. 빈 줄과 # 주석은 건너뛴다. 파일이 없으면 빈 dict."""
    values = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


@dataclass(frozen=True)
class Settings:
    """Postgres·S3(Bronze) 접속 정보. 모든 서비스는 127.0.0.1에서만 접속한다."""
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str
    bronze_bucket: str
    kafka_bootstrap: str = "127.0.0.1:59092"
    football_data_org_api_key: str | None = None  # 없으면 football-data.org 수집기를 실행할 수 없다(다른 기능은 동작)
    lake_bucket: str = "lake"   # 가공한 Parquet(Gold 내보내기·Spark 결과). 받은 원본만 두는 Bronze와 분리한다(4a)
    telegram_bot_token: str | None = None   # 없으면 알림을 보내지 않는다(NOT_CONFIGURED). 로그·오류에 찍지 않는다
    telegram_chat_id: str | None = None     # 알림을 받을 내 대화방 ID

    @property
    def postgres_conninfo(self) -> str:
        """psycopg.connect에 넘기는 접속 문자열. 컨테이너가 꺼져 있을 때 오래 멈추지 않도록 10초 안에 실패한다."""
        return (f"host={self.postgres_host} port={self.postgres_port} dbname={self.postgres_db} "
                f"user={self.postgres_user} password={self.postgres_password} connect_timeout=10")


def load_settings(env_path: Path = ENV_PATH) -> Settings:
    """.env와 환경 변수를 합쳐 Settings를 만든다. 필수 비밀번호가 없으면 안내와 함께 오류를 낸다."""
    values = {**read_env_file(env_path), **{key: value for key, value in os.environ.items()
                                            if key.startswith(("POSTGRES_", "S3_", "BRONZE_", "LAKE_", "KAFKA_", "FOOTBALL_DATA_ORG_", "TELEGRAM_"))}}
    missing = [key for key in ("POSTGRES_USER", "POSTGRES_PASSWORD", "S3_ACCESS_KEY", "S3_SECRET_KEY")
               if not values.get(key)]
    if missing:
        raise RuntimeError(f".env 설정이 없습니다({', '.join(missing)}). python -m pipelines.config init-env를 실행하세요.")
    # 기본은 이 PC(127.0.0.1)에서 접속하는 주소다. Airflow 컨테이너 안에서는 POSTGRES_HOST=postgres,
    # S3_ENDPOINT_URL=http://object-store:8333처럼 컨테이너 네트워크 주소로 바꿔 쓴다.
    return Settings(
        postgres_host=values.get("POSTGRES_HOST", "127.0.0.1"),
        postgres_port=int(values.get("POSTGRES_PORT", "55432")),
        postgres_db=values.get("POSTGRES_DB", "sports"),
        postgres_user=values["POSTGRES_USER"],
        postgres_password=values["POSTGRES_PASSWORD"],
        s3_endpoint_url=values.get("S3_ENDPOINT_URL", f"http://127.0.0.1:{values.get('S3_PORT', '59000')}"),
        s3_access_key=values["S3_ACCESS_KEY"],
        s3_secret_key=values["S3_SECRET_KEY"],
        bronze_bucket=values.get("BRONZE_BUCKET", "bronze"),
        # 컨테이너 안에서는 KAFKA_BOOTSTRAP=kafka:9092로 덮어쓴다.
        kafka_bootstrap=values.get("KAFKA_BOOTSTRAP", f"127.0.0.1:{values.get('KAFKA_PORT', '59092')}"),
        football_data_org_api_key=(values.get("FOOTBALL_DATA_ORG_API_KEY") or "").strip() or None,
        lake_bucket=values.get("LAKE_BUCKET", "lake"),
        telegram_bot_token=(values.get("TELEGRAM_BOT_TOKEN") or "").strip() or None,
        telegram_chat_id=(values.get("TELEGRAM_CHAT_ID") or "").strip() or None,
    )


def init_env(env_path: Path = ENV_PATH) -> bool:
    """.env가 없을 때만 무작위 비밀번호로 새로 만든다. 만들었으면 True, 이미 있으면 False(덮어쓰지 않음).

    기존 .env를 덮어쓰면 이미 만든 DB 볼륨의 비밀번호와 어긋나 접속할 수 없게 되므로 절대 덮어쓰지 않는다.
    """
    if env_path.exists():
        return False
    template = (PROJECT_ROOT / ".env.example").read_text(encoding="utf-8")
    generated = template.replace("POSTGRES_PASSWORD=change-me", f"POSTGRES_PASSWORD={secrets.token_urlsafe(24)}")
    generated = generated.replace("S3_ACCESS_KEY=change-me-access", f"S3_ACCESS_KEY=pipeline-{secrets.token_hex(6)}")
    generated = generated.replace("S3_SECRET_KEY=change-me-secret", f"S3_SECRET_KEY={secrets.token_urlsafe(30)}")
    env_path.write_text(generated, encoding="utf-8")
    return True


if __name__ == "__main__":
    if sys.argv[1:] == ["init-env"]:
        print(".env를 만들었습니다." if init_env() else ".env가 이미 있어 그대로 둡니다.")
    else:
        print("사용법: python -m pipelines.config init-env")
        sys.exit(2)
