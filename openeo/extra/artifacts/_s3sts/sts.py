from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types_boto3_sts.client import STSClient

from openeo.extra.artifacts._s3sts.config import S3STSConfig
from openeo.extra.artifacts._s3sts.model import AWSSTSCredentials
from openeo.extra.artifacts.exceptions import ProviderSpecificException
from openeo.rest.auth.auth import BearerAuth
from openeo.rest.connection import Connection
from openeo.util import Rfc3339


class OpenEOSTSClient:
    def __init__(self, config: S3STSConfig):
        self.config = config

    def assume_from_openeo_connection(self, connection: Connection) -> AWSSTSCredentials:
        """
        Takes an OpenEO connection object and returns temporary credentials to interact with S3
        """
        auth = connection.auth
        assert auth is not None
        if not isinstance(auth, BearerAuth):
            raise ProviderSpecificException("Only connections that have BearerAuth can be used.")
        auth_token = auth.bearer.split("/")

        return AWSSTSCredentials.from_assume_role_response(
            self._get_sts_client().assume_role_with_web_identity(
                RoleArn=self._get_aws_access_role(),
                RoleSessionName=f"artifact-helper-{Rfc3339().now_utc()}",
                WebIdentityToken=auth_token[2],
                DurationSeconds=43200,
            )
        )

    def _get_sts_client(self) -> STSClient:
        return self.config.build_client("sts")

    def _get_aws_access_role(self) -> str:
        return self.config.role
