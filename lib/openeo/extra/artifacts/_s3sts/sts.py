from __future__ import annotations

import logging
import time
from random import randint
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types_boto3_sts.client import STSClient

from openeo.extra.artifacts._s3sts.config import S3STSConfig
from openeo.extra.artifacts._s3sts.model import AWSSTSCredentials
from openeo.extra.artifacts.exceptions import ProviderSpecificException
from openeo.rest.auth.auth import BearerAuth
from openeo.rest.connection import Connection
from openeo.util import Rfc3339

_log = logging.getLogger(__name__)


class OpenEOSTSClient:
    _MAX_STS_ATTEMPTS = 3

    def __init__(self, config: S3STSConfig):
        self.config = config

    def assume_from_openeo_connection(self, connection: Connection, attempt: int = 0) -> AWSSTSCredentials:
        """
        Takes an OpenEO connection object and returns temporary credentials to interact with S3
        """
        auth = connection.auth
        assert auth is not None
        if not isinstance(auth, BearerAuth):
            raise ProviderSpecificException("Only connections that have BearerAuth can be used.")
        auth_token = auth.bearer.split("/")

        try:
            # Do an API call with OpenEO to trigger a refresh of our token if it were stale.
            connection.describe_account()
            return AWSSTSCredentials.from_assume_role_response(
                self._get_sts_client().assume_role_with_web_identity(
                    RoleArn=self._get_aws_access_role(),
                    RoleSessionName=f"artifact-helper-{Rfc3339().now_utc()}",
                    WebIdentityToken=auth_token[2],
                    DurationSeconds=43200,
                )
            )
        except Exception as e:
            _log.warning("Failed to get credentials for STS access")

            if attempt < self._MAX_STS_ATTEMPTS:
                # backoff with jitter
                max_sleep_ms = 500 * (2**attempt)
                sleep_ms = randint(0, max_sleep_ms)
                _log.info(f"Retrying STS access in {sleep_ms} ms")
                time.sleep(sleep_ms / 1000.0)
                attempt += 1
                _log.info(f"Retrying to get credentials for STS access {attempt}/{self._MAX_STS_ATTEMPTS}")
                return self.assume_from_openeo_connection(connection, attempt)
            else:
                raise RuntimeError("Could not get credentials from STS") from e

    def _get_sts_client(self) -> STSClient:
        return self.config.build_client("sts")

    def _get_aws_access_role(self) -> str:
        return self.config.role
