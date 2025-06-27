import logging
from typing import Callable

"""
The trace helps to pass on an X-Request-ID header value in all requests made by a
boto3 call.
"""

TRACE_ID_KEY = "X-Request-ID"


def create_header_adder(request_id: str) -> Callable:
    def add_request_id_header(request, **kwargs) -> None:
        logger = logging.getLogger("openeo.extra.artifacts")
        signature_version = kwargs.get("signature_version", "unknown")
        if "query" in signature_version:
            logger.debug("Do not add trace header for requests using query parameters instead of headers")
            return
        logger.debug("Adding trace id: {request_id}")
        request.headers.add_header(TRACE_ID_KEY, request_id)

    return add_request_id_header


def add_trace_id(client, trace_id: str = "") -> None:
    header_adder = create_header_adder(trace_id)
    client.meta.events.register("before-sign.s3", header_adder)


def add_trace_id_as_query_parameter(url, trace_id: str) -> str:
    return f"{url}&{TRACE_ID_KEY}={trace_id}"
