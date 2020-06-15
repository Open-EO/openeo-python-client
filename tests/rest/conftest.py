import pytest

import openeo.internal.graphbuilder_040


@pytest.fixture(params=["0.4.0", "1.0.0"])
def api_version(request):
    return request.param


def reset_graphbuilder():
    # Reset 0.4.0 style graph builder
    openeo.internal.graphbuilder_040.GraphBuilder.id_counter = {}


@pytest.fixture(autouse=True)
def auto_reset():
    """Fixture to automatically reset builders, counters, ..."""
    reset_graphbuilder()
