from __future__ import annotations

import datetime
from typing import TYPE_CHECKING
from openeo.extra.artifacts._s3.model import AWSSTSCredentials
from openeo.extra.artifacts._s3.config import S3Config
from openeo.rest.auth.auth import BearerAuth
from openeo.rest.connection import Connection


class OpenEOSTSClient:
    def __init__(self, config: S3Config):
        self.config = config

    def assume_from_openeo_connection(self, conn: Connection) -> AWSSTSCredentials:
        """
        Takes an OpenEO connection object and returns temporary credentials to interact with S3
        """
        auth = conn.auth
        assert auth is not None
        if not isinstance(auth, BearerAuth):
            raise ValueError("Only connections that have BearerAuth can be used.")
        auth_token = auth.bearer.split('/')
        sts = self.config.build_client("sts")

        return AWSSTSCredentials.from_assume_role_response(
            sts.assume_role_with_web_identity(
                RoleArn=self._get_aws_access_role(),
                RoleSessionName=f"artifact-helper-{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d%H%M%S')}",
                WebIdentityToken=auth_token[2],
                DurationSeconds=43200,
            )
        )
    
    def _get_aws_access_role(self) -> str:
        return self.config.sts_role_arn
