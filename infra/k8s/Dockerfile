# Kubernetes용 파이프라인 이미지(로드맵 8a). compose 이미지(infra/pipeline)는 코드를 볼륨으로 연결하지만,
# 쿠버네티스에서는 이미지 하나가 그대로 배포 단위라 코드를 굽는다. 의존성은 compose 이미지와 같은 파일을 쓴다.
# - 루트가 아닌 사용자(uid 1000), 바이트코드 파일을 쓰지 않음(읽기 전용 루트 파일시스템에서 돌기 위해)
FROM python:3.12.14-slim-bookworm

COPY requirements-pipeline.txt /tmp/requirements-pipeline.txt
RUN pip install --no-cache-dir -r /tmp/requirements-pipeline.txt \
 && useradd --create-home --uid 1000 pipeline

COPY pipelines /opt/project/pipelines
USER 1000
WORKDIR /opt/project
ENV PYTHONPATH=/opt/project \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
