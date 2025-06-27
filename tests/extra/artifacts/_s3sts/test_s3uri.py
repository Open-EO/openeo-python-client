# noinspection PyProtectedMember
from openeo.extra.artifacts._s3sts.model import S3URI


def test_s3uri_serialization_is_idempotent():
    # GIVEN an S3 URI
    my_s3_uri = "s3://mybucket/my-key1"

    # WHEN we convert it to an S3URI object
    s3_obj = S3URI.from_str(my_s3_uri)

    # THEN getting the string value must result in same
    assert str(s3_obj) == my_s3_uri
