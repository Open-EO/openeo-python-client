from typing import Type

import pytest

from openeo.extra.artifacts import build_artifact_helper
from openeo.extra.artifacts.exceptions import (
    NoAdvertisedProviders,
    ProviderSpecificException,
    UnsupportedArtifactsType,
)


@pytest.mark.parametrize(
    (
        "description",
        "extra_api_capabilities",
        "expected_ex",
    ),
    [
        [
            """When using the new PresignedS3AssetUrls with ipd in place but not required config in place we should
            be on old behavior""",
            {},  # No extra capabilities
            NoAdvertisedProviders,
        ],
        [
            "When the backend advertises an unsupported provider it should raise an exception",
            {
                "artifacts": {
                    "providers": [
                        {
                            "config": {
                                "bucket": "openeo-artifacts",
                                "role": "arn:aws:iam::000000000000:role/S3Access",
                                "s3_endpoint": "https://s3.oeo.test",
                                "sts_endpoint": "https://sts.oeo.test",
                            },
                            "id": "s3",
                            "type": "NonExistingStorageProvider",
                        }
                    ]
                }
            },
            UnsupportedArtifactsType,
        ],
        [
            """When using the S3STS provider a connection requires to have authentication this is not present in this
            test mocking""",
            {
                "artifacts": {
                    "providers": [
                        {
                            "config": {
                                "bucket": "openeo-artifacts",
                                "role": "arn:aws:iam::000000000000:role/S3Access",
                                "s3_endpoint": "https://s3.oeo.test",
                                "sts_endpoint": "https://sts.oeo.test",
                            },
                            "id": "s3",
                            "type": "S3STSConfig",
                        }
                    ]
                }
            },
            ProviderSpecificException,
        ],
        [
            """An artifacts section without providers must raise no providers exception""",
            {"artifacts": {}},
            NoAdvertisedProviders,
        ],
        [
            """An artifacts section with an empty providers list must raise no providers exception""",
            {"artifacts": {"providers": []}},
            NoAdvertisedProviders,
        ],
        [
            """When using the S3STS provider a connection requires to have authentication this is not present in this
            test mocking""",
            {
                "artifacts": {
                    "providers": [
                        {
                            "config": {
                                "bucket": "openeo-artifacts",
                                "role": "arn:aws:iam::000000000000:role/S3Access",
                                "s3_endpoint": "https://s3.oeo.test",
                                "sts_endpoint": "https://sts.oeo.test",
                            },
                            "id": "s3",
                            "type": "S3STSConfig",
                        }
                    ]
                }
            },
            ProviderSpecificException,
        ],
    ],
)
def test_artifacts_raising_exceptions_when_required(
    clean_capabilities_cache,
    conn_with_extra_capabilities,
    description: str,
    extra_api_capabilities: dict,
    expected_ex: Type[BaseException],
):
    # Given no provided config then the client should raise exceptions when there is no appropriate way of
    # configuring an Artifact helper
    with pytest.raises(expected_ex):
        build_artifact_helper(conn_with_extra_capabilities, None)
