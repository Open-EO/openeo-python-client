# Hard coded at this time but this could be a builder that depending on connection builds a client for the Storage
from openeo.extra.artifacts.internal_s3.artifact_helper import S3ArtifactHelper as ArtifactHelper
"""
from openeo.extra.artifacts import ArtifactHelper

artifact_helper = ArtifactHelper.from_openeo_connection(connection)
storage_uri = artifact_helper.upload_file(object_name, src_file_path)
presigned_uri = artifact_helper.get_presigned_url(storage_uri)
"""