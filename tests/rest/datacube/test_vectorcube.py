import pytest
from openeo.internal.graph_building import PGNode
from openeo.rest.connection import Connection
import openeo


def test_raster_to_vector(con100):
    img = con100.load_collection("S2")
    vector_cube = img.raster_to_vector()
    vector_cube_tranformed = vector_cube.process_with_node(openeo.UDF("python source code", "Python"))

    assert vector_cube_tranformed.flat_graph() == {
        'loadcollection1': {
            'arguments': {
                'id': 'S2',
                'spatial_extent': None,
                'temporal_extent': None
            },
            'process_id': 'load_collection'
        },
        'rastertovector1': {
            'arguments': {
                'data': {'from_node': 'loadcollection1'}
            },
            'process_id': 'raster_to_vector'
        },
        'runudf1': {
            'arguments': {
                'data': {'from_node': 'rastertovector1'},
                'runtime': 'Python',
                'udf': 'python source code'
            },
            'process_id': 'run_udf',
            'result': True}
    }
