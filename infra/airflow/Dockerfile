# 로컬 오케스트레이션용 Airflow 이미지(로드맵 1d).
# - 파이프라인 라이브러리(psycopg, boto3, 모델용 numpy)는 Airflow 환경에 설치한다. Airflow 버전이 바뀌지 않게 함께 고정한다.
# - dbt는 의존성 충돌을 피하려고 별도 가상환경(/home/airflow/dbt-venv)에 설치한다.
# - Spark(4a)도 같은 이유로 별도 가상환경(/home/airflow/spark-venv)에 Spark 이미지와 같은 requirements-spark.txt로 설치한다.
#   Spark 4는 Java 17이 필요해 Debian 저장소의 OpenJDK 17(JRE, 화면 없는 판)을 함께 둔다.
#   Airflow가 Spark 컨테이너를 직접 띄우려면 docker.sock(사실상 PC 관리자 권한)을 넘겨야 해서 이 방식을 택했다.
# - 코드(dags, pipelines, dbt)는 이미지에 굽지 않고 docker-compose에서 볼륨으로 연결해, 수정하면 바로 반영된다.
FROM apache/airflow:3.3.2-python3.12

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements-dbt.txt /tmp/requirements-dbt.txt
COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "psycopg[binary]==3.3.6" "boto3==1.43.115" "numpy==2.5.4" \
 && python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt \
 && python -m venv /home/airflow/spark-venv \
 && /home/airflow/spark-venv/bin/pip install --no-cache-dir -r /tmp/requirements-spark.txt
# 7b: Spark 이미지와 같은 Iceberg·JDBC jar(판·SHA-512 고정)
COPY infra/spark/fetch_jars.py /tmp/fetch_jars.py
RUN python /tmp/fetch_jars.py /home/airflow/iceberg-jars

ENV DBT_EXECUTABLE=/home/airflow/dbt-venv/bin/dbt \
    SPARK_PYTHON=/home/airflow/spark-venv/bin/python \
    ICEBERG_JARS=/home/airflow/iceberg-jars \
    PYTHONPATH=/opt/project
