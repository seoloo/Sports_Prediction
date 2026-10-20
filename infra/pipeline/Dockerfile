# 상시 실행 파이프라인 서비스(football-data.org 수집기, Kafka 소비자) 이미지(로드맵 2단계).
# 코드는 이미지에 굽지 않고 docker-compose에서 읽기 전용 볼륨으로 연결한다.
FROM python:3.12.14-slim-bookworm

COPY requirements-pipeline.txt /tmp/requirements-pipeline.txt
RUN pip install --no-cache-dir -r /tmp/requirements-pipeline.txt \
 && useradd --create-home --uid 1000 pipeline

USER pipeline
WORKDIR /opt/project
ENV PYTHONPATH=/opt/project \
    PYTHONUNBUFFERED=1
