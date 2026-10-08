from typing import Dict, Optional, Type

from openeo import Connection
from openeo.extra.artifacts._artifact_helper_abc import ArtifactHelperABC
from openeo.extra.artifacts._backend import ArtifactCapabilities
from openeo.extra.artifacts._config import ArtifactsStorageConfigABC

# noinspection PyProtectedMember
from openeo.extra.artifacts._s3sts.artifact_helper import S3STSArtifactHelper

# noinspection PyProtectedMember
from openeo.extra.artifacts._s3sts.config import S3STSConfig
from openeo.extra.artifacts.exceptions import UnsupportedArtifactsType

config_to_helper: Dict[Type[ArtifactsStorageConfigABC], Type[ArtifactHelperABC]] = {S3STSConfig: S3STSArtifactHelper}
config_type_to_helper: Dict[str, Type[ArtifactHelperABC]] = {
    ArtifactsStorageConfigABC.get_type_from(cfg): helper for cfg, helper in config_to_helper.items()
}


def build_artifact_helper(
    connection: Connection, config: Optional[ArtifactsStorageConfigABC] = None
) -> ArtifactHelperABC:
    """
    :param connection: ``openeo.Connection``  connection to an openEOBackend
    :param config: Optional **This parameter should only be used when instructed by the maintainer of the OpenEO
                   backend.** object to specify configuration for Artifact storage.  If omitted the helper will try to
                   get the preferred config as advertised by the OpenEO backend.

    :return: An Artifact helper instance that can be used to manage artifacts
    """
    if config is None:
        config_type = ArtifactCapabilities(connection).get_preferred_artifacts_provider().get_type()
    else:
        config_type = config.get_type()

    try:
        artifact_helper = config_type_to_helper[config_type]
        return artifact_helper.from_openeo_connection(
            connection, ArtifactCapabilities(connection).get_preferred_artifacts_provider(), config=config
        )
    except KeyError as ke:
        raise UnsupportedArtifactsType(config_type) from ke
