import json

import mock

from openeo.rest.datacube import DataCube


def get_download_graph(cube: DataCube, *, drop_save_result: bool = False, drop_load_collection: bool = False) -> dict:
    """
    Do fake download of a cube and intercept the process graph
    :param cube: cube to download
    :param drop_save_result: whether to drop the save_result node
    :param drop_load_collection: whether to drop the load_collection node
    :return:
    """
    # TODO: move this to a testing utility module
    with mock.patch.object(cube.connection, 'download') as download:
        cube.download("out.geotiff", format="GTIFF")
        download.assert_called_once()
        args, kwargs = download.call_args
    actual_graph = _json_normalize(args[0])
    if drop_save_result:
        save_results_nodes = [k for k, v in actual_graph.items() if v["process_id"] == "save_result"]
        assert len(save_results_nodes) == 1
        del actual_graph[save_results_nodes[0]]
    if drop_load_collection:
        actual_graph = {k: v for (k, v) in actual_graph.items() if v["process_id"] != "load_collection"}
    return actual_graph


def get_execute_graph(cube: DataCube) -> dict:
    """
    Do fake execute of a cube and intercept the process graph
    :param cube: cube to download
    :param connection: connection object
    :return:
    """
    with mock.patch.object(cube.connection, 'execute') as execute:
        cube.execute()
        execute.assert_called_once()
        args, kwargs = execute.call_args
    actual_graph = _json_normalize(args[0])
    return actual_graph


def _json_normalize(x: dict) -> dict:
    """Normalize given graph to be JSON compatible (e.g. use lists instead of tuples)"""
    return json.loads(json.dumps(x))
