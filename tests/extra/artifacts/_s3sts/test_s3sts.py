from __future__ import annotations

import dataclasses
import datetime
import logging
import os
from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING, Iterator, Optional
from unittest.mock import Mock

import boto3
import moto
import pytest

if TYPE_CHECKING:
    from types_boto3_sts.type_defs import AssumeRoleWithWebIdentityResponseTypeDef

from openeo import Connection
from openeo.extra.artifacts import build_artifact_helper

# noinspection PyProtectedMember
from openeo.extra.artifacts._s3sts.artifact_helper import S3STSArtifactHelper

# noinspection PyProtectedMember
from openeo.extra.artifacts._s3sts.config import S3STSConfig

# noinspection PyProtectedMember
from openeo.extra.artifacts._s3sts.sts import OpenEOSTSClient
from openeo.rest.auth.auth import BearerAuth
from tests.rest.conftest import API_URL

fake_creds_response: AssumeRoleWithWebIdentityResponseTypeDef = {
    "Credentials": {
        "AccessKeyId": "akid",
        "SecretAccessKey": "secret",
        "SessionToken": "token",
        # TODO: go for datetime.datetime.now(tz=datetime.UTC) once 3.10 support is no longer needed
        "Expiration": datetime.datetime.utcnow() + datetime.timedelta(hours=1),
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
def advertised_s3sts_config() -> Iterator[dict]:
    yield deepcopy(test_p_config)


@pytest.fixture
def extra_api_capabilities(advertised_s3sts_config: Optional[dict]) -> Iterator[dict]:
    if advertised_s3sts_config is None:
        yield {}
    else:
        yield {"artifacts": {"providers": [{"config": advertised_s3sts_config, "id": "s3", "type": "S3STSConfig"}]}}


@pytest.fixture
def conn_with_s3sts_capabilities(
    requests_mock, extra_api_capabilities, advertised_s3sts_config
) -> Iterator[Connection]:
    requests_mock.get(API_URL, json={"api_version": "1.0.0", **extra_api_capabilities})
    requests_mock.get(f"{API_URL}me", json={})
    conn = Connection(API_URL)
    conn.auth = BearerAuth("oidc/fake/token")
    yield conn


def test_backend_provided_settings_s3sts(clean_capabilities_cache, conn_with_s3sts_capabilities, mocked_sts):
    # Given a backend that exposes s3sts capabilities (fixture)
    # When creating an artifacthelper without specifying config
    ah = build_artifact_helper(conn_with_s3sts_capabilities, None)
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


def get_s3sts_config_default_field_values() -> dict:
    c = S3STSConfig()
    return {field_name.name: getattr(c, field_name.name) for field_name in dataclasses.fields(c)}


@pytest.mark.parametrize(
    [
        "description",
        "overrides",
        "advertised_s3sts_config",
    ],
    [
        [
            "When there is advertised configurations the explicit values must take precedence",
            {
                "s3_endpoint": test_c_s3_endpoint,
                "sts_endpoint": test_c_sts_endpoint,
                "role": test_c_role_arn,
                "bucket": test_c_bucket_name,
                "feature_flags": {"X-Proxy-Head-As-Get": True},
            },
            deepcopy(test_p_config),
        ],
        [
            "When there is no advertised configurations the explicit values should still be used.",
            {
                "s3_endpoint": test_c_s3_endpoint,
                "sts_endpoint": test_c_sts_endpoint,
                "role": test_c_role_arn,
                "bucket": test_c_bucket_name,
                "feature_flags": {"X-Proxy-Head-As-Get": True},
            },
            None,  # No actual config
        ],
    ],
)
def test_config_overrides_take_precedence(
    clean_capabilities_cache, conn_with_s3sts_capabilities, mocked_sts, overrides: dict, description: str
):
    defaults = get_s3sts_config_default_field_values()
    expected_values = {**defaults, **test_p_config, **overrides}

    # Given a backend that exposes s3sts capabilities (fixture)
    # When creating an artifacthelper without specifying config
    ah = build_artifact_helper(conn_with_s3sts_capabilities, S3STSConfig(**overrides))
    # Then the artifact helper is of the expected instance
    assert isinstance(ah, S3STSArtifactHelper)
    # Then the config is of the expected type
    assert isinstance(ah.config, S3STSConfig)
    # And the config contains the backend provided settings
    for field_name in dataclasses.fields(ah.config):
        assert expected_values[field_name.name] == getattr(ah.config, field_name.name)


@pytest.mark.parametrize(
    ["advertised_s3sts_config", "overrides"],
    [
        [  # Unknown feature flag advertised by backend
            {
                "s3_endpoint": test_c_s3_endpoint,
                "sts_endpoint": test_c_sts_endpoint,
                "role": test_c_role_arn,
                "bucket": test_c_bucket_name,
                "feature_flags": {"non-existing-feature-flag": "bar"},
            },
            {},
        ],
        [  # Unknown feature flag in config override
            {
                "s3_endpoint": test_c_s3_endpoint,
                "sts_endpoint": test_c_sts_endpoint,
                "role": test_c_role_arn,
                "bucket": test_c_bucket_name,
                "feature_flags": {"X-Proxy-Head-As-Get": True},
            },
            {"feature_flags": {"non-existing-feature-flag": "bar"}},
        ],
    ],
)
def test_unknown_feature_flag_must_emit_warning(
    clean_capabilities_cache, advertised_s3sts_config, conn_with_s3sts_capabilities, mocked_sts, caplog, overrides
):
    caplog.set_level(logging.INFO)
    # Given a backend that exposes s3sts capabilities but with a feature flag unknown to the client (fixture)
    # When creating an artifacthelper without specifying config
    ah = build_artifact_helper(conn_with_s3sts_capabilities, S3STSConfig(**overrides))
    # Then the artifact helper is of the expected instance
    assert isinstance(ah, S3STSArtifactHelper)
    # Then the config is of the expected type
    assert isinstance(ah.config, S3STSConfig)
    # Then a warning was emitted that client is likely not up-to-date enough
    expected_log = "Update your client"
    assert expected_log in caplog.text


@pytest.fixture
def test_file(tmp_path) -> Iterator[Path]:
    tempfile = tmp_path / "temp_file.txt"
    tempfile.write_text("hello world.")
    yield tempfile


@pytest.fixture
def s3_endpoint() -> str:
    return test_c_s3_endpoint


@pytest.fixture
def mock_s3_access(s3_endpoint):
    # Given mocked S3 interaction
    os.environ["MOTO_S3_CUSTOM_ENDPOINTS"] = s3_endpoint
    with moto.mock_aws():
        # Given artifacts bucket exists
        boto3.client("s3").create_bucket(Bucket=test_c_bucket_name)
        yield


@pytest.mark.parametrize(
    ["advertised_s3sts_config", "has_extra_query_parameter"],
    [
        [
            {
                "s3_endpoint": test_c_s3_endpoint,
                "sts_endpoint": test_c_sts_endpoint,
                "role": test_c_role_arn,
                "bucket": test_c_bucket_name,
                "feature_flags": {"X-Proxy-Head-As-Get": True},
            },
            True,
        ],
        [
            {
                "s3_endpoint": test_c_s3_endpoint,
                "sts_endpoint": test_c_sts_endpoint,
                "role": test_c_role_arn,
                "bucket": test_c_bucket_name,
                "feature_flags": {"X-Proxy-Head-As-Get": False},
            },
            False,
        ],
        [
            {
                "s3_endpoint": test_c_s3_endpoint,
                "sts_endpoint": test_c_sts_endpoint,
                "role": test_c_role_arn,
                "bucket": test_c_bucket_name,
            },
            False,
        ],
    ],
)
def test_feature_flag_x_proxy_head_as_get(
    clean_capabilities_cache,
    advertised_s3sts_config,
    conn_with_s3sts_capabilities,
    mocked_sts,
    has_extra_query_parameter,
    test_file,
    mock_s3_access,
):
    # Given mocked S3 interaction and default bucket (fixture)
    # Given a backend that exposes s3sts capabilities
    # When creating an artifacthelper without specifying config
    ah = build_artifact_helper(conn_with_s3sts_capabilities)
    # When uploading a file and getting a presigned url
    s3_uri = ah.upload_file(test_file)
    presigned = ah.get_presigned_url(s3_uri)

    if has_extra_query_parameter:
        # THEN the query parameter must be set to true if flag is enabled
        assert "X-Proxy-Head-As-Get=True" in presigned
    else:
        # THEN We shouldn't pass the parameter at all when the flag is disabled otherwise the S3API might choke on it
        assert "X-Proxy-Head-As-Get" not in presigned


@pytest.mark.parametrize(
    ["advertised_s3sts_config", "has_checksum_support"],
    [
        [
            {
                "s3_endpoint": test_c_s3_endpoint,
                "sts_endpoint": test_c_sts_endpoint,
                "role": test_c_role_arn,
                "bucket": test_c_bucket_name,
                "feature_flags": {"checksum-via-trailers": True},
            },
            True,
        ],
        [
            {
                "s3_endpoint": test_c_s3_endpoint,
                "sts_endpoint": test_c_sts_endpoint,
                "role": test_c_role_arn,
                "bucket": test_c_bucket_name,
                "feature_flags": {"checksum-via-trailers": False},
            },
            False,
        ],
        [
            {
                "s3_endpoint": test_c_s3_endpoint,
                "sts_endpoint": test_c_sts_endpoint,
                "role": test_c_role_arn,
                "bucket": test_c_bucket_name,
            },
            False,
        ],
    ],
)
def test_feature_flag_checksum_via_trailers(
    clean_capabilities_cache,
    advertised_s3sts_config,
    conn_with_s3sts_capabilities,
    mocked_sts,
    has_checksum_support,
    mock_s3_access,  # Mock this boto3 client creation otherwise does requests
):
    # Given a backend that exposes s3sts capabilities
    # When creating an artifacthelper without specifying config
    ah = build_artifact_helper(conn_with_s3sts_capabilities)

    assert isinstance(ah, S3STSArtifactHelper)
    # Then the config is of the expected type
    assert isinstance(ah.config, S3STSConfig)

    s3_client = ah.config.build_client("s3")
    if has_checksum_support:
        # If an operation supports checksumming then checksum because backend supports it anyway
        assert s3_client._client_config.request_checksum_calculation == "when_supported"
    else:
        # Only do checksumming if really required as this likely fails since support is unconfirmed
        assert s3_client._client_config.request_checksum_calculation == "when_required"
