from typing import Dict, Optional, Type

from openeo import Connection
from openeo.extra.artifacts._s3sts.artifact_helper import S3STSArtifactHelper
from openeo.extra.artifacts._s3sts.config import S3STSConfig
from openeo.extra.artifacts.artifact_helper_abc import (
    ArtifactHelperABC,
    ArtifactHelperBuilderABC,
)
from openeo.extra.artifacts.backend import ArtifactCapabilities
from openeo.extra.artifacts.config import StorageConfig
from openeo.extra.artifacts.exceptions import UnsupportedArtifactsType

config_to_helper: Dict[Type[StorageConfig], Type[ArtifactHelperABC]] = {S3STSConfig: S3STSArtifactHelper}
config_type_to_helper: Dict[str, Type[ArtifactHelperABC]] = {
    StorageConfig.get_type_from(cfg): helper for cfg, helper in config_to_helper.items()
}

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
        if config is None:
            config_type = ArtifactCapabilities(conn).get_preferred_artifacts_provider().get_type()
        else:
            config_type = config.get_type()

        try:
            artifact_helper = config_type_to_helper[config_type]
            return artifact_helper.from_openeo_connection(
                conn, ArtifactCapabilities(conn).get_preferred_artifacts_provider(), config=config
            )
        except KeyError as ke:
            raise UnsupportedArtifactsType(config_type) from ke
