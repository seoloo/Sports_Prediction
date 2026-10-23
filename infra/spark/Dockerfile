# Spark 작업 이미지(로드맵 4a). 단일 컨테이너 local 모드로 Gold 특징을 다시 계산한다.
# - Spark 4는 Java 17 이상이 필요하다. Debian 저장소의 OpenJDK 17(JRE, 화면 없는 판)을 쓴다.
# - procps: Spark 실행 스크립트가 프로세스 확인에 ps를 쓴다.
# - Iceberg(7b): Spark용 Iceberg 런타임·AWS 번들·Postgres JDBC jar를 Maven Central에서 받아 SHA-512를 확인한다
#   (infra/spark/fetch_jars.py). 세션이 ICEBERG_JARS 폴더의 jar를 불러 Iceberg JDBC 카탈로그로 같은 테이블을 읽고 쓴다.
# - 코드는 이미지에 굽지 않고 docker-compose에서 읽기 전용 볼륨으로 연결한다.
FROM python:3.12.14-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN pip install --no-cache-dir -r /tmp/requirements-spark.txt \
 && useradd --create-home --uid 1000 spark
COPY infra/spark/fetch_jars.py /tmp/fetch_jars.py
RUN python /tmp/fetch_jars.py /opt/iceberg-jars

USER spark
WORKDIR /opt/project
ENV PYTHONPATH=/opt/project \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python \
    ICEBERG_JARS=/opt/iceberg-jars
