from __future__ import annotations

import io
import os
from datetime import datetime, timezone
from uuid import uuid4

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError


class FakeStreamingBody:
    def __init__(self, payload: bytes) -> None:
        self._buffer = io.BytesIO(payload)

    def read(self, size: int = -1) -> bytes:
        return self._buffer.read(size)

    def iter_lines(self) -> list[bytes]:
        return self._buffer.getvalue().splitlines()

    def close(self) -> None:
        self._buffer.close()


class FakeS3Paginator:
    def __init__(self, objects: dict[str, dict[str, object]]) -> None:
        self._objects = objects

    def paginate(self, *, Bucket: str, Prefix: str) -> list[dict[str, object]]:
        contents = []
        for key, metadata in sorted(self._objects.items()):
            if not key.startswith(Prefix):
                continue
            contents.append(
                {
                    "Key": key,
                    "Size": len(metadata["Body"]),
                    "LastModified": metadata["LastModified"],
                }
            )
        return [{"Contents": contents}]


class FakeS3Client:
    def __init__(self, objects: dict[str, dict[str, object]]) -> None:
        self._objects = objects

    def get_paginator(self, operation_name: str) -> FakeS3Paginator:
        assert operation_name == "list_objects_v2"
        return FakeS3Paginator(self._objects)

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        assert Bucket == "demo-bucket"
        metadata = self._objects.get(Key)
        if metadata is None:
            raise self._client_error("NoSuchKey")
        return {
            "Body": FakeStreamingBody(metadata["Body"]),
            "ETag": metadata["ETag"],
            "LastModified": metadata["LastModified"],
        }

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: object,
        IfNoneMatch: str | None = None,
    ) -> dict[str, object]:
        assert Bucket == "demo-bucket"
        if IfNoneMatch == "*" and Key in self._objects:
            raise self._client_error("PreconditionFailed")

        payload = Body.read() if hasattr(Body, "read") else Body
        if not isinstance(payload, bytes):
            payload = bytes(payload)
        self._objects[Key] = {
            "Body": payload,
            "LastModified": datetime.now(timezone.utc),
            "ETag": self._etag(payload),
        }
        return {"ETag": self._objects[Key]["ETag"]}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        assert Bucket == "demo-bucket"
        metadata = self._objects.get(Key)
        if metadata is None:
            raise self._client_error("404")
        return {
            "ETag": metadata["ETag"],
            "LastModified": metadata["LastModified"],
        }

    def delete_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        assert Bucket == "demo-bucket"
        self._objects.pop(Key, None)
        return {}

    def _etag(self, payload: bytes) -> str:
        return f'"{len(payload):x}-{sum(payload):x}"'

    def _client_error(self, code: str) -> ClientError:
        return ClientError({"Error": {"Code": code, "Message": code}}, "fake_s3")


@pytest.fixture
def fake_s3_installer(monkeypatch: pytest.MonkeyPatch):
    def install(objects: dict[str, bytes]) -> FakeS3Client:
        client = FakeS3Client(
            {
                key: {
                    "Body": payload,
                    "LastModified": datetime(2026, 1, 2, tzinfo=timezone.utc),
                    "ETag": f'"{len(payload):x}-{sum(payload):x}"',
                }
                for key, payload in objects.items()
            }
        )
        monkeypatch.setattr(
            "fluxel.core.storage.boto3.client", lambda service_name: client
        )
        monkeypatch.setattr(
            "fluxel.core.repository_store.boto3.client",
            lambda service_name: client,
        )
        return client

    return install


def _integration_config() -> dict[str, str]:
    endpoint = os.getenv("FLUXEL_MINISTACK_ENDPOINT")
    if not endpoint:
        pytest.skip("Set FLUXEL_MINISTACK_ENDPOINT to run S3 integration tests")

    access_key = os.getenv("FLUXEL_MINISTACK_ACCESS_KEY") or os.getenv(
        "AWS_ACCESS_KEY_ID"
    )
    secret_key = os.getenv("FLUXEL_MINISTACK_SECRET_KEY") or os.getenv(
        "AWS_SECRET_ACCESS_KEY"
    )
    if not access_key or not secret_key:
        pytest.skip(
            "Set FLUXEL_MINISTACK_ACCESS_KEY/SECRET_KEY or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY"
        )

    return {
        "endpoint": endpoint,
        "access_key": access_key,
        "secret_key": secret_key,
        "region": os.getenv("FLUXEL_MINISTACK_REGION", "us-east-1"),
    }


@pytest.fixture
def ministack_client(monkeypatch: pytest.MonkeyPatch):
    config = _integration_config()
    client = boto3.client(
        "s3",
        endpoint_url=config["endpoint"],
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        region_name=config["region"],
        config=Config(s3={"addressing_style": "path"}),
    )
    try:
        client.list_buckets()
    except (EndpointConnectionError, BotoCoreError, ClientError) as error:
        pytest.skip(f"S3 integration endpoint unavailable: {error}")

    monkeypatch.setattr("fluxel.core.storage.boto3.client", lambda service_name: client)
    monkeypatch.setattr(
        "fluxel.core.repository_store.boto3.client", lambda service_name: client
    )
    return client


@pytest.fixture
def s3_repo_root(ministack_client) -> str:
    bucket = f"fluxel-it-{uuid4().hex[:20]}"
    prefix = f"repos/{uuid4().hex}"
    ministack_client.create_bucket(Bucket=bucket)
    try:
        yield f"s3://{bucket}/{prefix}"
    finally:
        continuation_token: str | None = None
        while True:
            kwargs = {"Bucket": bucket}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            response = ministack_client.list_objects_v2(**kwargs)
            contents = response.get("Contents", [])
            if contents:
                ministack_client.delete_objects(
                    Bucket=bucket,
                    Delete={
                        "Objects": [{"Key": item["Key"]} for item in contents],
                        "Quiet": True,
                    },
                )
            if not response.get("IsTruncated"):
                break
            continuation_token = response.get("NextContinuationToken")
        ministack_client.delete_bucket(Bucket=bucket)
