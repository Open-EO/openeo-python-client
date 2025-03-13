from __future__ import annotations
import hashlib
from dataclasses import dataclass
from openeo.extra.artifacts.uri import StorageURI


@dataclass(frozen=True)
class AWSSTSCredentials:
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_SESSION_TOKEN: str
    subject_from_web_identity_token: str

    @classmethod
    def from_assume_role_response(cls, resp: dict) -> AWSSTSCredentials:
        d = resp["Credentials"]
        return AWSSTSCredentials(
            AWS_ACCESS_KEY_ID=d["AccessKeyId"],
            AWS_SECRET_ACCESS_KEY=d["SecretAccessKey"],
            AWS_SESSION_TOKEN=d["SessionToken"],
            subject_from_web_identity_token=resp["SubjectFromWebIdentityToken"]
        )

    def as_kwargs(self) -> dict:
        return {
            "aws_access_key_id": self.AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": self.AWS_SECRET_ACCESS_KEY,
            "aws_session_token": self.AWS_SESSION_TOKEN
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
        s3_prefix = "s3://"
        if uri.startswith(s3_prefix):
            without_prefix = uri[len(s3_prefix):]
            without_prefix_parts = without_prefix.split("/")
            bucket = without_prefix_parts[0]
            if len(without_prefix_parts) == 1:
                return S3URI(bucket, "")
            else:
                return S3URI(bucket, "/".join(without_prefix_parts[1:]))
        else:
            raise ValueError(f"Input {uri} is not a valid S3 URI should be of form s3://<bucket>/<key>")

    def __str__(self):
        return f"s3://{self.bucket}/{self.key}"
