from dataclasses import dataclass
import boto3
import botocore

from openeo import Connection
from openeo.extra.artifacts.internal_s3.tracer import add_trace_id, add_trace_id_as_query_parameter
from openeo.extra.artifacts.config import StorageConfig
from botocore.config import Config
from typing import Optional
from packaging.version import Version


if Version(botocore.__version__) < Version("1.36.0"):
    # Before 1.36 checksuming was not done by default anyway and therefore
    # there was no opt-out.
    no_default_checksum_cfg = Config()
else:
    no_default_checksum_cfg = Config(
        request_checksum_calculation='when_required',
    )


@dataclass
class S3Config(StorageConfig):
    """The s3 endpoint url protocol:://fqdn[:portnumber]"""
    s3_endpoint_url: Optional[str] = None
    """The sts endpoint url protocol:://fqdn[:portnumber]"""
    sts_endpoint_url: Optional[str] = None
    """The trace_id is if you want to send a uuid4 identifier to the backend"""
    trace_id: str = ""
    """You can change the botocore_config used but this is an expert option"""
    botocore_config: Optional[Config] = None
    """The role ARN to be assumed"""
    sts_role_arn: Optional[str] = None

    def _load_openeo_connection_metadata(self, conn: Connection) -> None:
        """
        Hard coding since connection does not allow automatic determining config yet.
        """
        if self.s3_endpoint_url is None:
            self.s3_endpoint_url = "https://s3.waw3-1.openeo.v1.dataspace.copernicus.eu"

        if self.sts_endpoint_url is None:
            self.sts_endpoint_url = "https://sts.waw3-1.openeo.v1.dataspace.copernicus.eu"

        if self.sts_role_arn is None:
            self.sts_role_arn = "arn:aws:iam::000000000000:role/S3Access"

    def __post_init__(self):
        self.botocore_config = self.botocore_config or no_default_checksum_cfg

    def build_client(self, service_name: str, session_kwargs: Optional[dict] = None):
        """
        Build a boto3 client for an OpenEO service provider.

        service_name is the service you want to consume: s3|sts
        session_kwargs: a dictionary with keyword arguments that will be passed when creating the boto session
        """
        session_kwargs = session_kwargs or {}
        session = boto3.Session(region_name=self._get_storage_region(), **session_kwargs)
        client = session.client(
            service_name, 
            endpoint_url=self._get_endpoint_url(service_name),
            config=self.botocore_config,   
        )
        if self.trace_id != "":
            add_trace_id(client, self.trace_id)
        return client

    @staticmethod
    def _remove_protocol_from_uri(uri: str):
        uri_separator = "://"
        idx = uri.find(uri_separator)
        if idx < 0:
            raise ValueError("_remove_protocol_from_uri must be of form protocol://...")
        return uri[idx+len(uri_separator):]
    
    def _get_storage_region(self) -> str:
        """
        S3 URIs follow the convention detailed on https://docs.aws.amazon.com/general/latest/gr/s3.html
        """
        s3_names = ["s3", "s3-fips"]
        reserved_words = ["dualstack", "prod", "stag", "dev"]
        s3_endpoint_parts = self._remove_protocol_from_uri(self.s3_endpoint_url).split(".")
        for s3_name in s3_names:
            try:
                old_idx = s3_endpoint_parts.index(s3_name)
                idx = old_idx + 1
                while idx != old_idx:
                    old_idx = idx
                    for reserved_word in reserved_words:
                        if s3_endpoint_parts[idx] in reserved_word:
                            idx += 1
                return s3_endpoint_parts[idx]
            except ValueError:
                continue
        raise ValueError(f"Cannot determine region from {self.s3_endpoint_url}")

    def _get_endpoint_url(self, service_name: str) -> str:
        if service_name == "s3":
            return self.s3_endpoint_url
        elif service_name == "sts":
            return self.sts_endpoint_url
        raise ValueError(f"Unsupported service {service_name}")

    def add_trace_id_qp_if_needed(self, url: str) -> str:
        if self.trace_id == "":
            return url
        return add_trace_id_as_query_parameter(url, self.trace_id)

    def get_sts_role_arn(self) -> str:
        return self.sts_role_arn
