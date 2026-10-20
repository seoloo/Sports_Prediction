# 로컬 오케스트레이션용 Airflow 이미지(로드맵 1d).
# - 파이프라인 라이브러리(psycopg, boto3)는 Airflow 환경에 설치한다. Airflow 버전이 바뀌지 않게 함께 고정한다.
# - dbt는 의존성 충돌을 피하려고 별도 가상환경(/home/airflow/dbt-venv)에 설치한다.
# - 코드(dags, pipelines, dbt)는 이미지에 굽지 않고 docker-compose에서 볼륨으로 연결해, 수정하면 바로 반영된다.
FROM apache/airflow:3.3.2-python3.12

COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "psycopg[binary]==3.3.6" "boto3==1.43.115" \
 && python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt

ENV DBT_EXECUTABLE=/home/airflow/dbt-venv/bin/dbt \
    PYTHONPATH=/opt/project
