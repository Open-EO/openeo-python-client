from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types_boto3_sts.type_defs import AssumeRoleWithWebIdentityResponseTypeDef

import hashlib
from dataclasses import dataclass
from urllib.parse import urlparse

from openeo.extra.artifacts._uri import StorageURI


@dataclass(frozen=True)
class AWSSTSCredentials:
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: str
    subject_from_web_identity_token: str
    expiration: datetime.datetime

    @classmethod
    def from_assume_role_response(cls, resp: AssumeRoleWithWebIdentityResponseTypeDef) -> AWSSTSCredentials:
        d = resp["Credentials"]
        return AWSSTSCredentials(
            aws_access_key_id=d["AccessKeyId"],
            aws_secret_access_key=d["SecretAccessKey"],
            aws_session_token=d["SessionToken"],
            subject_from_web_identity_token=resp["SubjectFromWebIdentityToken"],
            expiration=d["Expiration"],
        )

    def as_kwargs(self) -> dict:
        return {
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
            "aws_session_token": self.aws_session_token,
        }

    def get_user_hash(self) -> str:
        hash_object = hashlib.sha1(self.subject_from_web_identity_token.encode())
        return hash_object.hexdigest()


@dataclass(frozen=True)
class S3URI(StorageURI):
    bucket: str
    key: str

    @classmethod
    def from_str(cls, uri: str) -> S3URI:
        _parsed = urlparse(uri, allow_fragments=False)
        if _parsed.scheme != "s3":
            raise ValueError(f"Input {uri} is not a valid S3 URI should be of form s3://<bucket>/<key>")
        bucket = _parsed.netloc
        if _parsed.query:
            key = _parsed.path.lstrip("/") + "?" + _parsed.query
        else:
            key = _parsed.path.lstrip("/")

        return S3URI(bucket, key)

    def to_string(self) -> str:
        return f"s3://{self.bucket}/{self.key}"
