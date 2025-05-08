import inspect

import pytest

from openeo import BatchJob, Connection, DataCube, VectorCube
from openeo.internal.documentation import (
    assert_same_main_description,
    assert_same_param_docs,
    assert_same_return_docs,
    extract_main_description,
    extract_params,
    extract_return,
)
from openeo.rest.mlmodel import MlModel
from openeo.rest.stac_resource import StacResource

DOCBLOCK1 = """
    The description

    and more

    :param a: description of a
    :param b_b : multi-line description
        of b

    :return: the result
        of the operation

    That's it!
"""

def test_extract_params():
    assert extract_params(DOCBLOCK1) == {
        "a": "description of a",
        "b_b": "multi-line description\n    of b",
    }


def test_extract_return():
    assert extract_return(DOCBLOCK1) == "the result\n    of the operation"


def test_extract_main_description():
    assert extract_main_description(DOCBLOCK1) == ["The description", "and more"]


@pytest.mark.parametrize(
    ["method_a", "method_b"],
    [
        # Connection vs DataCube
        (Connection.download, DataCube.download),
        (Connection.create_job, DataCube.create_job),
        # Compare DataCube methods internally
        (DataCube.download, DataCube.create_job),
        (DataCube.download, DataCube.execute_batch),
        (DataCube.create_job, DataCube.execute_batch),
        # DataCube vs BatchJob
        (BatchJob.start_and_wait, DataCube.execute_batch),
        # DataCube vs VectorCube
        (DataCube.download, VectorCube.download),
        (DataCube.create_job, VectorCube.create_job),
        (DataCube.execute_batch, VectorCube.execute_batch),
        (DataCube.save_result, VectorCube.save_result),
        (DataCube.validate, VectorCube.validate),
        # DataCube vs MlModel
        (DataCube.create_job, MlModel.create_job),
        (DataCube.execute_batch, MlModel.execute_batch),
        (DataCube.validate, MlModel.validate),
        # DataCube vs StacResource
        (DataCube.download, StacResource.download),
        (DataCube.create_job, StacResource.create_job),
        (DataCube.execute_batch, StacResource.execute_batch),
        (DataCube.validate, StacResource.validate),
    ],
)
def test_cube_processing_params_and_return(method_a, method_b):
    """Check params/return of cube download/execute related methods"""
    signature_a = inspect.signature(method_a)
    signature_b = inspect.signature(method_b)

    only_intersection = set(signature_a.parameters.keys()) != set(signature_b.parameters.keys())
    assert_same_param_docs(method_a, method_b, only_intersection=only_intersection)

    if signature_a.return_annotation == signature_b.return_annotation:
        assert_same_return_docs(method_a, method_b)


@pytest.mark.parametrize(
    ["method_a", "method_b"],
    [
        # Connection vs DataCube
        (Connection.download, DataCube.download),
        (Connection.create_job, DataCube.create_job),
        # DataCube vs VectorCube
        (DataCube.download, VectorCube.download),
        (DataCube.create_job, VectorCube.create_job),
        (DataCube.execute_batch, VectorCube.execute_batch),
        (DataCube.save_result, VectorCube.save_result),
        # DataCube vs MlModel
        (DataCube.create_job, MlModel.create_job),
        (DataCube.execute_batch, MlModel.execute_batch),
        # DataCube vs StacResource
        (DataCube.download, StacResource.download),
        (DataCube.create_job, StacResource.create_job),
        (DataCube.execute_batch, StacResource.execute_batch),
    ],
)
def test_cube_processing_description(method_a, method_b):
    """Check main description of cube download/execute related methods"""
    assert_same_main_description(method_a, method_b)
