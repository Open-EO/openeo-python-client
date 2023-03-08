import datetime
import logging
from pathlib import Path
import xarray as xr
from typing import Dict, List, Tuple, Union, Callable, Optional, Any, Iterator

from openeo.metadata import CollectionMetadata
from openeo.internal.graph_building import PGNode, as_flat_graph
from openeo.rest.datacube import DataCube
from openeo.internal.jupyter import VisualDict, VisualList
from openeo.local.collections import _get_local_collections, _get_netcdf_zarr_metadata, _get_geotiff_metadata
from openeo.local.processing import PROCESS_REGISTRY
from openeo_pg_parser_networkx.graph import OpenEOProcessGraph

class LocalConnection():
    """
    Connection to no backend, for local processing.
    """

    def __init__(self,local_collections_path: Union[str,List]):
        """
        Constructor of LocalConnection.

        :param local_collections_path: String or list of strings, path to the folder(s) with
        the local collections in netCDF, geoTIFF or ZARR.
        """
        self.local_collections_path = local_collections_path
        
    def list_collections(self) -> List[dict]:
        """
        List basic metadata of all collections provided in the local collections folder.

        .. caution::
        :return: list of dictionaries with basic collection metadata.
        """
        data = _get_local_collections(self.local_collections_path)["collections"]
        return VisualList("collections", data=data)
    
    def describe_collection(self, collection_id: str) -> dict:
        """
        Get full collection metadata for given collection id.
        
        .. seealso::
        
            :py:meth:`~openeo.rest.connection.Connection.list_collection_ids`
            to list all collection ids provided by the back-end.

        :param collection_id: collection id
        :return: collection metadata.
        """
        local_collection = Path(collection_id)
        if '.nc' in local_collection.suffixes or '.zarr' in local_collection.suffixes:
            data = _get_netcdf_zarr_metadata(local_collection)
        elif '.tif' in local_collection.suffixes or '.tiff' in local_collection.suffixes:
            data = _get_geotiff_metadata(local_collection)
        return VisualDict("collection", data=data)
    
    def collection_metadata(self, name) -> CollectionMetadata:
        # TODO: duplication with `Connection.describe_collection`: deprecate one or the other?
        return CollectionMetadata(metadata=self.describe_collection(name))
    
    def load_collection(
            self,
            collection_id: str,
            spatial_extent: Optional[Dict[str, float]] = None,
            temporal_extent: Optional[List[Union[str, datetime.datetime, datetime.date]]] = None,
            bands: Optional[List[str]] = None,
            properties: Optional[Dict[str, Union[str, PGNode, Callable]]] = None,
            fetch_metadata=True,
    ) -> DataCube:
        """
        Load a DataCube by collection id.

        :param collection_id: image collection identifier
        :param spatial_extent: limit data to specified bounding box or polygons
        :param temporal_extent: limit data to specified temporal interval
        :param bands: only add the specified bands
        :param properties: limit data by metadata property predicates
        :return: a datacube containing the requested data
        """
        return DataCube.load_collection(
            collection_id=collection_id, connection=self,
            spatial_extent=spatial_extent, temporal_extent=temporal_extent, bands=bands, properties=properties,
            fetch_metadata=fetch_metadata,
        )

    def execute(self, process_graph: Union[dict, str, Path]) -> xr.DataArray:
        """
        Execute locally the process graph and return the result as an xarray.DataArray.

        :param process_graph: (flat) dict representing a process graph, or process graph as raw JSON string,
        :return: a datacube containing the requested data
        """
        process_graph = as_flat_graph(process_graph)
        return OpenEOProcessGraph(process_graph).to_callable(PROCESS_REGISTRY)()