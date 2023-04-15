import pytest


@pytest.fixture(params=["1.0.0"])
def api_version(request):
    return request.param
