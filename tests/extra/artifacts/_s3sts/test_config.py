import pytest

# noinspection PyProtectedMember
from openeo.extra.artifacts._s3sts.config import S3STSConfig


@pytest.mark.parametrize(
    "s3_endpoint_uri,expected",
    [
        ("https://s3.us-east-2.amazonaws.com", "us-east-2"),
        ("https://s3.dualstack.us-east-2.amazonaws.com", "us-east-2"),
        ("https://s3-fips.dualstack.us-east-2.amazonaws.com", "us-east-2"),
        ("https://s3-fips.us-east-2.amazonaws.com", "us-east-2"),
        ("https://s3.waw3-1.openeo.v1.dataspace.copernicus.eu", "waw3-1"),
        ("https://s3.prod.waw3-1.openeo.v1.dataspace.copernicus.eu", "waw3-1"),
        ("https://s3.stag.waw3-1.openeo.v1.dataspace.copernicus.eu", "waw3-1"),
        ("https://s3.dev.waw3-1.openeo.v1.dataspace.copernicus.eu", "waw3-1"),
    ],
)
def test_region_should_be_derived_correctly(s3_endpoint_uri: str, expected: str):
    # GIVEN config with a certain endpoint
    config = S3STSConfig(s3_endpoint=s3_endpoint_uri)

    # WHEN a region is extracted
    region = config._get_storage_region()

    # THEN is is the expected
    assert region == expected, f"Got {region}, expected {expected}"


def test_extracting_region_from_invalid_url_must_give_value_error():
    # GIVEN a config with invalid url
    config = S3STSConfig(s3_endpoint="https://www.google.com")

    # WHEN a region is extracted
    # THEN a Value error is raised
    with pytest.raises(ValueError):
        config._get_storage_region()
