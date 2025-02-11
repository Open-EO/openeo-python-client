import pytest

from openeo import DataCube, VectorCube
from openeo.internal.documentation import assert_same_param_docs, extract_params
from openeo.rest.mlmodel import MlModel
from openeo.rest.stac_resource import StacResource


def test_extract_params():
    assert (
        extract_params(
            """
                The description

                and more

                :param a: description of a
                :param b_b : multi-line description
                    of b

                That's it!
                """
        )
        == {
            "a": "description of a",
            "b_b": "multi-line description\n    of b",
        }
    )




@pytest.mark.parametrize(
    ["method_a", "method_b", "only_intersection"],
    [
        # Compare DataCube methods internally
        (DataCube.download, DataCube.create_job, True),
        (DataCube.download, DataCube.execute_batch, True),
        (DataCube.create_job, DataCube.execute_batch, True),
        # DataCube vs VectorCube
        (DataCube.download, VectorCube.download, False),
        (DataCube.create_job, VectorCube.create_job, False),
        (DataCube.execute_batch, VectorCube.execute_batch, False),
        (DataCube.save_result, VectorCube.save_result, False),
        # DataCube vs MlModel
        (DataCube.create_job, MlModel.create_job, True),
        (DataCube.execute_batch, MlModel.execute_batch, True),
        # DataCube vs StacResource
        (DataCube.download, StacResource.download, True),
        (DataCube.create_job, StacResource.create_job, True),
        (DataCube.execute_batch, StacResource.execute_batch, True),
    ],
)
def test_compare_download_execute_params(method_a, method_b, only_intersection):
    assert_same_param_docs(method_a, method_b, only_intersection=only_intersection)
    # TODO: compare return description
