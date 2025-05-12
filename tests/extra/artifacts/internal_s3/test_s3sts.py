from __future__ import annotations

import dataclasses
import datetime
from typing import TYPE_CHECKING, Iterator
from unittest.mock import Mock

import pytest

if TYPE_CHECKING:
    from types_boto3_sts.type_defs import AssumeRoleWithWebIdentityResponseTypeDef

from openeo import Connection
from openeo.extra.artifacts import ArtifactHelper
from openeo.extra.artifacts._s3sts.artifact_helper import S3STSArtifactHelper
from openeo.extra.artifacts._s3sts.config import S3STSConfig
from openeo.extra.artifacts._s3sts.sts import OpenEOSTSClient
from openeo.rest.auth.auth import BearerAuth
from tests.rest.conftest import API_URL

fake_creds_response: AssumeRoleWithWebIdentityResponseTypeDef = {
    "Credentials": {
        "AccessKeyId": "akid",
        "SecretAccessKey": "secret",
        "SessionToken": "token",
        "Expiration": datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(hours=1),
    },
    "SubjectFromWebIdentityToken": "tokensubject",
    "AssumedRoleUser": {"AssumedRoleId": "1", "Arn": "not important"},
    "PackedPolicySize": 10,
    "Provider": "notImportant",
    "Audience": "notImportant",
    "SourceIdentity": "",
    "ResponseMetadata": {
        "RequestId": "0000-00",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {},
        "RetryAttempts": 0,
        "HostId": "1",
    },
}


@pytest.fixture
def mocked_sts(monkeypatch):
    mocked_sts_client = Mock(["assume_role_with_web_identity"])
    mocked_sts_client.assume_role_with_web_identity.return_value = fake_creds_response
    monkeypatch.setattr(OpenEOSTSClient, "_get_sts_client", Mock(return_value=mocked_sts_client))
    yield mocked_sts_client


test_p_bucket_name = "openeo-artifacts"
test_p_role_arn = "arn:aws:iam::000000000000:role/S3Access"
test_p_s3_endpoint = "https://s3.oeo.test"
test_p_sts_endpoint = "https://sts.oeo.test"
test_p_config = {
    "bucket": test_p_bucket_name,
    "role": test_p_role_arn,
    "s3_endpoint": test_p_s3_endpoint,
    "sts_endpoint": test_p_sts_endpoint,
}


@pytest.fixture
def conn_with_stss3_capabilities(requests_mock, extra_api_capabilities) -> Iterator[Connection]:
    extra_api_capabilities = {"artifacts": {"providers": [{"cfg": test_p_config, "id": "s3", "type": "S3STSConfig"}]}}
    requests_mock.get(API_URL, json={"api_version": "1.0.0", **extra_api_capabilities})
    conn = Connection(API_URL)
    conn.auth = BearerAuth("oidc/fake/token")
    yield conn


def test_backend_provided_settings_s3sts(clean_capabilities_cache, conn_with_stss3_capabilities, mocked_sts):
    # Given a backend that exposes stss3 capabilities (fixture)
    # When creating an artifacthelper without specifying config
    ah = ArtifactHelper.from_openeo_connection(conn_with_stss3_capabilities, None)
    # Then the artifact helper is of the expected instance
    assert isinstance(ah, S3STSArtifactHelper)
    # Then the config is of the expected type
    assert isinstance(ah.config, S3STSConfig)
    # And the config contains the backend provided settings
    assert test_p_bucket_name == ah.config.bucket
    assert test_p_role_arn == ah.config.role
    assert test_p_sts_endpoint == ah.config.sts_endpoint
    assert test_p_s3_endpoint == ah.config.s3_endpoint


# Custom overrides
test_c_bucket_name = "openeo-custom-artifacts"
test_c_role_arn = "arn:aws:iam::000000000000:role/S3Artifacts"
test_c_s3_endpoint = "https://s3.oeo2.test"
test_c_sts_endpoint = "https://sts.oeo2.test"


def get_stss3_config_default_field_values() -> dict:
    c = S3STSConfig()
    return {field_name.name: getattr(c, field_name.name) for field_name in dataclasses.fields(c)}


@pytest.mark.parametrize(
    "overrides",
    [
        {
            "s3_endpoint": test_c_s3_endpoint,
            "sts_endpoint": test_c_sts_endpoint,
            "role": test_c_role_arn,
            "bucket": test_c_bucket_name,
        }
    ],
)
def test_config_overrides_take_precedence(
    clean_capabilities_cache, conn_with_stss3_capabilities, mocked_sts, overrides: dict
):
    defaults = get_stss3_config_default_field_values()
    expected_values = {**defaults, **test_p_config, **overrides}

    # Given a backend that exposes stss3 capabilities (fixture)
    # When creating an artifacthelper without specifying config
    ah = ArtifactHelper.from_openeo_connection(conn_with_stss3_capabilities, S3STSConfig(**overrides))
    # Then the artifact helper is of the expected instance
    assert isinstance(ah, S3STSArtifactHelper)
    # Then the config is of the expected type
    assert isinstance(ah.config, S3STSConfig)
    # And the config contains the backend provided settings
    for field_name in dataclasses.fields(ah.config):
        assert expected_values[field_name.name] == getattr(ah.config, field_name.name)
