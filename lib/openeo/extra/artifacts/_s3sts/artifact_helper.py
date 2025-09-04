from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from types_boto3_s3.client import S3Client

from pathlib import Path

from openeo.extra.artifacts._artifact_helper_abc import ArtifactHelperABC
from openeo.extra.artifacts._s3sts.config import S3STSConfig
from openeo.extra.artifacts._s3sts.model import S3URI, AWSSTSCredentials
from openeo.extra.artifacts._s3sts.sts import OpenEOSTSClient
from openeo.rest.connection import Connection


class S3STSArtifactHelper(ArtifactHelperABC):
    # From what size will we switch to multi-part-upload
    MULTIPART_THRESHOLD_IN_MB = 50

    def __init__(self, connection: Connection, config: S3STSConfig):
        super().__init__(config)
        self._connection = connection
        self.config = config
        self._creds = self.get_new_creds()
        self._s3: Optional[S3Client] = None

    @classmethod
    def _from_openeo_connection(cls, connection: Connection, config: S3STSConfig) -> S3STSArtifactHelper:
        return S3STSArtifactHelper(connection, config=config)

    def get_new_creds(self) -> AWSSTSCredentials:
        sts = OpenEOSTSClient(config=self.config)
        return sts.assume_from_openeo_connection(self._connection)

    def _user_prefix(self) -> str:
        """Each user has its own prefix retrieve it"""
        return self._creds.get_user_hash()

    def _get_upload_prefix(self) -> str:
        # TODO: replace utcnow when `datetime.datetime.now(datetime.UTC)` in oldest supported Python version
        return f"{self._user_prefix()}/{datetime.datetime.utcnow().strftime('%Y/%m/%d')}/"

    def _get_upload_key(self, object_name: str) -> str:
        return f"{self._get_upload_prefix()}{object_name}"

    @staticmethod
    def get_object_name_from_path(path: str | Path) -> str:
        if isinstance(path, str):
            path = Path(path)
        return path.name

    def _get_s3_client(self):
        # TODO: validate whether credentials are still reasonably long valid
        # and if not refresh credentials and rebuild client
        if self._s3 is None:
            self._s3 = self.config.build_client("s3", session_kwargs=self._creds.as_kwargs())
        return self._s3

    def upload_file(self, path: str | Path, object_name: str = "") -> S3URI:
        """
        Upload a file to a backend understanding the S3 API

        :param path A file path to the file that must be uploaded
        :param object_name: Optional the final part of the name to be uploaded. If omitted the filename is used.

        :return: `S3URI` A S3URI that points to the uploaded file in the S3 compatible backend
        """
        # Local import to avoid dependency
        from boto3.s3.transfer import TransferConfig

        mb = 1024**2
        config = TransferConfig(multipart_threshold=self.MULTIPART_THRESHOLD_IN_MB * mb)
        bucket = self.config.bucket
        key = self._get_upload_key(object_name or self.get_object_name_from_path(path))
        self._get_s3_client().upload_file(str(path), bucket, key, Config=config)
        return S3URI(bucket, key)

    def get_presigned_url(self, storage_uri: S3URI, expires_in_seconds: int = 7 * 3600 * 24) -> str:
        """
        Get a presigned URL to allow retrieval of an object.

        :param storage_uri `S3URI` A S3URI that points to the uploaded file in the S3 compatible backend
        :param expires_in_seconds: Optional the number of seconds the link is valid for (defaults to 7 days)

        :return: `str` A HTTP url that can be used to download a file. It also supports Range header in its requests.
        """
        url = self._get_s3_client().generate_presigned_url(
            "get_object",
            Params={"Bucket": storage_uri.bucket, "Key": storage_uri.key, **self.get_extra_sign_arguments()},
            ExpiresIn=expires_in_seconds,
        )
        assert isinstance(self._config, S3STSConfig)
        return self._config.add_trace_id_qp_if_needed(url)

    def get_extra_sign_arguments(self) -> dict:
        extra_sign_args = {}
        if self.config.is_feature_enabled("X-Proxy-Head-As-Get"):
            extra_sign_args["X-Proxy-Head-As-Get"] = True
        return extra_sign_args

    @classmethod
    def _get_default_storage_config(cls) -> S3STSConfig:
        return S3STSConfig()
