"""S3 호환 Bronze 저장소(로컬은 SeaweedFS).

원본은 내용 해시(SHA-256)를 경로에 넣어 저장한다(content-addressed).
같은 내용은 항상 같은 경로라서 여러 번 저장해도 결과가 같고(멱등), 이미 있으면 다시 올리지 않는다.
표준 S3 API(boto3)만 사용해 AWS S3나 다른 S3 호환 저장소로 바꿔도 이 코드는 그대로 쓸 수 있다.
"""
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from .config import Settings, load_settings


def client(settings: Settings | None = None):
    """S3 클라이언트. 로컬(127.0.0.1) 개발 환경이라 TLS 없이 접속하고, 경로 방식(path-style) 주소를 쓴다."""
    settings = settings or load_settings()
    return boto3.client(
        "s3", endpoint_url=settings.s3_endpoint_url, region_name="us-east-1",
        aws_access_key_id=settings.s3_access_key, aws_secret_access_key=settings.s3_secret_key,
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5, "mode": "standard"}))


def ensure_bucket(store, bucket: str) -> None:
    """버킷이 없으면 만든다."""
    try:
        store.head_bucket(Bucket=bucket)
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in ("404", "NoSuchBucket", "NotFound"):
            raise
        store.create_bucket(Bucket=bucket)


def object_exists(store, bucket: str, key: str) -> bool:
    """객체가 이미 있는지 확인한다. 없으면 False, 그 밖의 오류는 그대로 올린다."""
    try:
        store.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def put_if_absent(store, bucket: str, key: str, payload: bytes, metadata: dict,
                  content_type: str = "application/json") -> bool:
    """객체가 없을 때만 올린다. 새로 올렸으면 True, 이미 있었으면 False."""
    if object_exists(store, bucket, key):
        return False
    store.put_object(Bucket=bucket, Key=key, Body=payload, ContentType=content_type, Metadata=metadata)
    return True


def read_object(store, bucket: str, key: str) -> bytes:
    """저장한 원본을 다시 읽는다. 재처리와 검증에 쓴다."""
    return store.get_object(Bucket=bucket, Key=key)["Body"].read()
