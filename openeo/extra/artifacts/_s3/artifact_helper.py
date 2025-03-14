from __future__ import annotations

import datetime
from boto3.s3.transfer import TransferConfig
from pathlib import Path

from openeo.rest.connection import Connection
from openeo.extra.artifacts.artifact_helper_abc import ArtifactHelperABC
from openeo.extra.artifacts._s3.sts import OpenEOSTSClient
from openeo.extra.artifacts._s3.config import S3Config
from openeo.extra.artifacts._s3.model import S3URI
from openeo.extra.artifacts._s3.model import AWSSTSCredentials


class S3ArtifactHelper(ArtifactHelperABC):
    BUCKET_NAME = "openeo-artifacts"
    # From what size will we switch to multi-part-upload
    MULTIPART_THRESHOLD_IN_MB = 50

    def __init__(self, creds: AWSSTSCredentials, config: S3Config):
        super().__init__(config)
        self._creds = creds
        self.s3 = config.build_client("s3", session_kwargs=creds.as_kwargs())
    
    @classmethod
    def _from_openeo_connection(cls, conn: Connection, config: S3Config) -> S3ArtifactHelper:
        sts = OpenEOSTSClient(config=config)
        creds = sts.assume_from_openeo_connection(conn)
        return S3ArtifactHelper(creds, config=config)

    def _user_prefix(self) -> str:
        """Each user has its own prefix retrieve it"""
        return self._creds.get_user_hash()
    
    def _get_upload_prefix(self) -> str:
        return f"{self._user_prefix()}/{datetime.datetime.now(datetime.UTC).strftime('%Y/%m/%d')}/"
    
    def _get_upload_key(self, object_name: str) -> str:
        return f"{self._get_upload_prefix()}{object_name}"

    @staticmethod
    def get_object_name_from_path(path: str | Path) -> str:
        if isinstance(path, str):
            path = Path(path)
        return path.name

    def upload_file(self, path: str | Path, object_name: str = "") -> S3URI:
        """
        Upload a file to a backend understanding the S3 API

        :param path A file path to the file that must be uploaded
        :param object_name: Optional the final part of the name to be uploaded. If omitted the filename is used.

        :return: `S3URI` A S3URI that points to the uploaded file in the S3 compatible backend
        """
        mb = 1024 ** 2
        config = TransferConfig(multipart_threshold=self.MULTIPART_THRESHOLD_IN_MB * mb)
        bucket = self.BUCKET_NAME
        key = self._get_upload_key(object_name or self.get_object_name_from_path(path))
        self.s3.upload_file(
            str(path),
            bucket,
            key,
            Config=config
        )
        return S3URI(bucket, key)
    
    def get_presigned_url(self, storage_uri: S3URI, expires_in_seconds: int = 7 * 3600 * 24) -> str:
        """
        Get a presigned URL to allow retrieval of an object.

        :param storage_uri `S3URI` A S3URI that points to the uploaded file in the S3 compatible backend
        :param expires_in_seconds: Optional the number of seconds the link is valid for (defaults to 7 days)

        :return: `str` A HTTP url that can be used to download a file. It also supports Range header in its requests.
        """
        url = self.s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': storage_uri.bucket, 'Key': storage_uri.key},
            ExpiresIn=expires_in_seconds
        )
        assert isinstance(self._config, S3Config)
        return self._config.add_trace_id_qp_if_needed(url)

    @classmethod
    def _get_default_storage_config(cls) -> S3Config:
        return S3Config()
