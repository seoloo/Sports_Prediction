"""Spark용 Iceberg jar를 내려받고 SHA-512를 확인한다(로드맵 7b). Spark·Airflow 이미지를 만들 때 RUN으로 부른다.

- 받는 곳은 Maven Central뿐이고, 판과 SHA-512는 여기에 고정한다. 값이 다르면 이미지 빌드가 실패한다.
- Iceberg 두 개는 Maven에 공개된 SHA-512, Postgres JDBC 드라이버는 Maven에 SHA-512가 없어 공개 SHA-1·MD5와 맞는 것을
  확인한 파일에서 계산한 값이다(2026-10-24).
사용: python fetch_jars.py <저장할 폴더>
"""
import hashlib
import sys
import urllib.request
from pathlib import Path

MAVEN = "https://repo1.maven.org/maven2"
JARS = {
    "iceberg-spark-runtime-4.1_2.13-1.12.0.jar": (
        "org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0",
        "ac1e2cb00970309f0c6bf241fabf334aef373c1f3e3b443181d429d5f2c0903ace47df9cfc1d55c95807a096cf405ae0a8868831b9e9eb5696dfdb6380587f38"),
    "iceberg-aws-bundle-1.12.0.jar": (
        "org/apache/iceberg/iceberg-aws-bundle/1.12.0",
        "b01456f80142c8837004e31329a0f40db8a9d44917a4a694cb7d8d6d3a68cb20c61e6f7788fa1bdcb95c0edf724322b49667d6f9ef60fc9cc46da5c24ce5c7de"),
    "postgresql-42.7.14.jar": (
        "org/postgresql/postgresql/42.7.14",
        "2a7c94c3f86f7ad6cfa16a964fe79dec8b122449679c8bbdb18f7052593349c2d11dd07992034de88d6effd840e2edf5ea8542e947de19905c75677d0dd44abc"),
}


def main(target: str) -> None:
    folder = Path(target)
    folder.mkdir(parents=True, exist_ok=True)
    for name, (path, expected) in JARS.items():
        with urllib.request.urlopen(f"{MAVEN}/{path}/{name}", timeout=120) as response:
            payload = response.read()
        actual = hashlib.sha512(payload).hexdigest()
        if actual != expected:
            sys.exit(f"{name}: SHA-512가 다릅니다({actual[:16]}… ≠ {expected[:16]}…). 받은 파일을 쓰지 않습니다.")
        (folder / name).write_bytes(payload)
        print(f"{name}: {len(payload):,} bytes, SHA-512 확인")


if __name__ == "__main__":
    main(sys.argv[1])
