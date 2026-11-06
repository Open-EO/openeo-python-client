from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional
from urllib.parse import urlencode

from openeo.extra.artifacts._backend import ProviderConfig
from openeo.extra.artifacts._config import ArtifactsStorageConfigABC
from openeo.extra.artifacts._s3sts.tracer import (
    add_trace_id,
    add_trace_id_as_query_parameter,
)
from openeo.extra.artifacts.exceptions import NoDefaultConfig

if TYPE_CHECKING:
    from botocore.config import Config
    from types_boto3_s3.client import S3Client

_log = logging.getLogger(__name__)


DISABLE_TRACING_TRACE_ID = "00000000-0000-0000-0000-000000000000"


def _default_feature_flags() -> dict:
    """
    These holds the default values for supported feature flags. This is also a good place to document them.
    """
    return {
        # X-Proxy-Head-As-Get when True instructs the S3Client to add an additional query parameter for presigned URLs
        # When the S3 backend supports this query parameter the presigned URL can be used for both HEAD and GET HTTP
        # calls which is required by some tooling. This is behind a feature flag as most S3 backends do not support this
        "X-Proxy-Head-As-Get": False,
        # Whether the S3 endpoint supports checksums being sent as trailers over HTTP. If it is not supported it could
        # give confusing error messages or worse the trailer can be perceived as data and end up in the resulting file
        # leading to data corruption. Ironically if it is supported it helps to improve data integrity so for those
        # backends it is best that they enable it.
        "checksum-via-trailers": False,
    }


@dataclass(frozen=True)
class S3STSConfig(ArtifactsStorageConfigABC):
    """The s3 endpoint url protocol:://fqdn[:portnumber]"""

    s3_endpoint: Optional[str] = None
    """The sts endpoint url protocol:://fqdn[:portnumber]"""
    sts_endpoint: Optional[str] = None
    """The trace_id is if you want to send a uuid4 identifier to the backend"""
    trace_id: str = DISABLE_TRACING_TRACE_ID
    """The role ARN to be assumed"""
    role: Optional[str] = None
    """The bucket to store the object into"""
    bucket: Optional[str] = None
    """Feature flags for S3STS interaction"""
    feature_flags: dict = field(default_factory=_default_feature_flags)

    def _load_connection_provided_config(self, provider_config: ProviderConfig) -> None:
        if self.s3_endpoint is None:
            object.__setattr__(self, "s3_endpoint", provider_config["s3_endpoint"])

        if self.sts_endpoint is None:
            object.__setattr__(self, "sts_endpoint", provider_config["sts_endpoint"])

        if self.role is None:
            object.__setattr__(self, "role", provider_config["role"])

        if self.bucket is None:
            object.__setattr__(self, "bucket", provider_config["bucket"])

        feature_flags = self.feature_flags
        try:
            advertised_feature_flags = provider_config["feature_flags"]
            if not isinstance(advertised_feature_flags, dict):
                raise ValueError(
                    "Advertised feature flags for S3STSConfig must be dict. Contact OpenEO backend provider"
                )
            for flag in [k for k in advertised_feature_flags.keys()]:
                feature_flags[flag] = advertised_feature_flags[flag]
            object.__setattr__(self, "feature_flags", feature_flags)
        except NoDefaultConfig:
            pass  # If no feature flags are advertised stick to the defaults

        # Emit warning when
        for flag in feature_flags.keys():
            if flag not in _default_feature_flags():
                _log.warning(
                    f"Backend advertised unknown feature flag {flag}. "
                    "Update your client to make sure it is correctly handled"
                )

    def should_trace(self) -> bool:
        return self.trace_id != DISABLE_TRACING_TRACE_ID

    @staticmethod
    def _allow_custom_params_for_get_object(s3_client: S3Client) -> None:
        """
        Allow passing custom parameters into the S3 client get_object api calls.

        Custom parameters should start with "x-" to be considered a custom parameter.

        This call is idempotent and can be done multiple times on a single client.
        """
        custom_params = "custom_params"

        def client_param_handler(*, params, context, **_kw):
            def is_custom(k):
                return k.lower().startswith("x-")

            # Store custom parameters in context for later event handlers
            context[custom_params] = {k: v for k, v in params.items() if is_custom(k)}
            # Remove custom parameters from client parameters,
            # because validation would fail on them
            return {k: v for k, v in params.items() if not is_custom(k)}

        def request_param_injector(*, request, **_kw):
            """https://stackoverflow.com/questions/59056522/create-a-presigned-s3-url-for-get-object-with-custom-logging-information-using-b"""
            if request.context[custom_params]:
                request.url += "&" if "?" in request.url else "?"
                request.url += urlencode(request.context[custom_params])

        provide_client_params = "provide-client-params.s3.GetObject"
        s3_client.meta.events.register(
            provide_client_params, client_param_handler, f"{provide_client_params}-paramhandler"
        )
        before_sign = "before-sign.s3.GetObject"
        s3_client.meta.events.register(before_sign, request_param_injector, f"{before_sign}-paraminjector")

    def _get_botocore_config(self) -> Config:
        from botocore.config import Config  # Local import to avoid dependency

        if self.is_feature_enabled("checksum-via-trailers"):
            return Config()
        else:
            # If the backend does not allow checksums in the trailers we must configure boto3 to only
            # do checksuming when explicitly required. Starting boto 1.36.0 checksum is done by default
            # but this could fail if the S3 endpoint does not support it.
            return Config(
                request_checksum_calculation="when_required",
            )

    def build_client(self, service_name: str, session_kwargs: Optional[dict] = None):
        """
        Build a boto3 client for an OpenEO service provider.

        service_name is the service you want to consume: s3|sts
        session_kwargs: a dictionary with keyword arguments that will be passed when creating the boto session
        """
        import boto3  # Local import to avoid unnecessary dependency

        session_kwargs = session_kwargs or {}
        session = boto3.Session(region_name=self._get_storage_region(), **session_kwargs)
        client = session.client(
            service_name,
            endpoint_url=self._get_endpoint_url(service_name),
            config=self._get_botocore_config(),
        )
        self._allow_custom_params_for_get_object(client)
        if self.should_trace():
            add_trace_id(client, self.trace_id)
        return client

    @staticmethod
    def _remove_protocol_from_uri(uri: str):
        uri_separator = "://"
        idx = uri.find(uri_separator)
        if idx < 0:
            raise ValueError("_remove_protocol_from_uri must be of form protocol://...")
        return uri[idx + len(uri_separator) :]

    def is_feature_enabled(self, feature_flag: str) -> bool:
        try:
            return self.feature_flags[feature_flag]
        except KeyError:
            return _default_feature_flags()[feature_flag]

    def _get_storage_region(self) -> str:
        """
        S3 URIs follow the convention detailed on https://docs.aws.amazon.com/general/latest/gr/s3.html
        """
        s3_names = ["s3", "s3-fips"]
        reserved_words = ["dualstack", "prod", "stag", "dev"]
        s3_endpoint_parts = self._remove_protocol_from_uri(self.s3_endpoint).split(".")
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
        raise ValueError(f"Cannot determine region from {self.s3_endpoint}")

    def _get_endpoint_url(self, service_name: str) -> str:
        if service_name == "s3":
            return self.s3_endpoint
        elif service_name == "sts":
            return self.sts_endpoint
        raise ValueError(f"Unsupported service {service_name}")

    def add_trace_id_qp_if_needed(self, url: str) -> str:
        if not self.should_trace():
            return url
        return add_trace_id_as_query_parameter(url, self.trace_id)

    def get_sts_role_arn(self) -> str:
        return self.role
