from typing import Optional

from openeo import Connection
from openeo.extra.artifacts.artifact_helper_abc import ArtifactHelperBuilderABC, ArtifactHelperABC
from openeo.extra.artifacts._s3.artifact_helper import S3ArtifactHelper
from openeo.extra.artifacts.config import StorageConfig


class ArtifactHelper(ArtifactHelperBuilderABC):
    @classmethod
    def from_openeo_connection(cls, conn: Connection, config: Optional[StorageConfig] = None) -> ArtifactHelperABC:
        """
        Create an artifactHelper for an openEO backend.

        :param conn ``openeo.Connection``  connection to an openEOBackend
        :param config: Optional object to specify configuration for Artifact storage

        :return: An Artifact helper based on info provided by the backend .

        Example usage:
        ```
        from openeo.extra.artifacts import ArtifactHelper

        artifact_helper = ArtifactHelper.from_openeo_connection(connection)
        storage_uri = artifact_helper.upload_file(object_name, src_file_path)
        presigned_uri = artifact_helper.get_presigned_url(storage_uri)
        ```
        """
        # At time of writing there is only one type of artifact store supported so no resolving done yet.
        return S3ArtifactHelper.from_openeo_connection(conn, config)
