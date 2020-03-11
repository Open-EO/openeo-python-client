import os
from pathlib import Path
from typing import Union

import mock
from openeo.rest.datacube import DataCube
from openeo.rest.imagecollectionclient import ImageCollectionClient


def get_test_resource(relative_path):
    dir = Path(os.path.dirname(os.path.realpath(__file__)))
    return str(dir / relative_path)


def load_json_resource(relative_path):
    import json
    with open(get_test_resource(relative_path), 'r+') as f:
        return json.load(f)


def get_download_graph(cube: Union[DataCube, ImageCollectionClient]) -> dict:
    """
    Do fake download of a cube and intercept the process graph
    :param cube: cube to download
    :param connection: connection object
    :return:
    """
    with mock.patch.object(cube.connection, 'download') as download:
        cube.download("out.geotiff", format="GTIFF")
        download.assert_called_once()
        args, kwargs = download.call_args
        actual_graph = args[0]
    return actual_graph
