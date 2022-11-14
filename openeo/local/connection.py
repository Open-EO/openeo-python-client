import datetime
import xarray as xr
import numpy as np
import rioxarray
from glob import glob
from pathlib import Path
from pyproj import Transformer
from typing import Dict, List, Tuple, Union, Callable, Optional, Any, Iterator

from openeo.metadata import CollectionMetadata
from openeo.internal.graph_building import PGNode
from openeo.rest.datacube import DataCube
from openeo.internal.jupyter import VisualDict, VisualList

def _get_temporal_dimension(dims):
    if 't' in dims:
        return 't'
    elif 'time' in dims:
        return 'time'
    elif 'temporal' in dims:
        return 'temporal'
    elif 'DATE' in dims:
        return 'DATE'
    else:
        return None

def _get_x_spatial_dimension(dims):
    if 'x' in dims:
        return 'x'
    elif 'X' in dims:
        return 'X'
    elif 'lon' in dims:
        return 'lon'
    elif 'longitude' in dims:
        return 'longitude'
    else:
        return None

def _get_y_spatial_dimension(dims):
    if 'y' in dims:
        return 'y'
    elif 'Y' in dims:
        return 'Y'
    elif 'lat' in dims:
        return 'lat'
    elif 'latitude' in dims:
        return 'latitude'
    else:
        return None

def _get_netcdf_metadata(file_path):
    data = xr.open_dataset(file_path,chunks={})
    t_dim = _get_temporal_dimension(data.dims)
    x_dim = _get_x_spatial_dimension(data.dims)
    y_dim = _get_y_spatial_dimension(data.dims)

    metadata = {}
    metadata['stac_version'] = '1.0.0-rc.2'
    metadata['type'] = 'Collection'
    metadata['id'] = file_path
    data_attrs_lowercase = [x.lower() for x in data.attrs]
    data_attrs_original  = [x for x in data.attrs]
    data_attrs = dict(zip(data_attrs_lowercase,data_attrs_original))
    if 'title' in data_attrs_lowercase:
        metadata['title'] = data.attrs[data_attrs['title']]
    else:
        metadata['title'] = file_path
    if 'description' in data_attrs_lowercase:
        metadata['description'] = data.attrs[data_attrs['description']]
    else:
        metadata['description'] = ''
    if 'license' in data_attrs_lowercase:
        metadata['license'] = data.attrs[data_attrs['license']]
    else:
        metadata['license'] = ''
    providers = [{'name':'',
                 'roles':['producer'],
                 'url':''}]
    if 'providers' in data_attrs_lowercase:
        providers[0]['name'] = data.attrs[data_attrs['providers']]
        metadata['providers'] = providers
    elif 'institution' in data_attrs_lowercase:
        providers[0]['name'] = data.attrs[data_attrs['institution']]
        metadata['providers'] = providers
    else:
        metadata['providers'] = providers
    if 'links' in data_attrs_lowercase:
        metadata['links'] = data.attrs[data_attrs['links']]
    else:
        metadata['links'] = ''
    x_min = data[x_dim].min().item(0)
    x_max = data[x_dim].max().item(0)
    y_min = data[y_dim].min().item(0)
    y_max = data[y_dim].max().item(0)

    crs_present = False
    bands = list(data.data_vars)
    if 'crs' in bands:
        bands.remove('crs')
        crs_present = True

    if crs_present:
        if 'crs_wkt' in data.crs.attrs:
            transformer = Transformer.from_crs(data.crs.attrs['crs_wkt'], "epsg:4326")
            lat_min,lon_min = transformer.transform(x_min,y_min)
            lat_max,lon_max = transformer.transform(x_max,y_max)               
            extent = {'spatial': {'bbox': [[lon_min, lat_min, lon_max, lat_max]]}}
            if t_dim is not None:
                t_min = str(data[t_dim].min().values)
                t_max = str(data[t_dim].max().values)
                extent['temporal'] = {'interval': [[t_min,t_max]]}

            metadata['extent'] = extent

    t_dimension = {}
    if t_dim is not None:
        t_dimension = {t_dim: {'type': 'temporal', 'extent':[t_min,t_max]}}

    x_dimension = {x_dim: {'type': 'spatial','axis':'x','extent':[x_min,x_max]}}
    y_dimension = {y_dim: {'type': 'spatial','axis':'y','extent':[y_min,y_max]}}
    if crs_present:
        if 'crs_wkt' in data.crs.attrs:
            x_dimension[x_dim]['reference_system'] = data.crs.attrs['crs_wkt']
            y_dimension[y_dim]['reference_system'] = data.crs.attrs['crs_wkt']

    b_dimension = {}
    if len(bands)>0:
        b_dimension = {'bands': {'type': 'bands', 'values':bands}}

    metadata['cube:dimensions'] = {**t_dimension,**x_dimension,**y_dimension,**b_dimension}

    return metadata

def _get_geotiff_metadata(file_path):
    data = rioxarray.open_rasterio(file_path,chunks={})

    t_dim = _get_temporal_dimension(data.dims)
    x_dim = _get_x_spatial_dimension(data.dims)
    y_dim = _get_y_spatial_dimension(data.dims)

    metadata = {}
    metadata['stac_version'] = '1.0.0-rc.2'
    metadata['type'] = 'Collection'
    metadata['id'] = file_path
    data_attrs_lowercase = [x.lower() for x in data.attrs]
    data_attrs_original  = [x for x in data.attrs]
    data_attrs = dict(zip(data_attrs_lowercase,data_attrs_original))
    if 'title' in data_attrs_lowercase:
        metadata['title'] = data.attrs[data_attrs['title']]
    else:
        metadata['title'] = file_path
    if 'description' in data_attrs_lowercase:
        metadata['description'] = data.attrs[data_attrs['description']]
    else:
        metadata['description'] = ''
    if 'license' in data_attrs_lowercase:
        metadata['license'] = data.attrs[data_attrs['license']]
    else:
        metadata['license'] = ''
    providers = [{'name':'',
                 'roles':['producer'],
                 'url':''}]
    if 'providers' in data_attrs_lowercase:
        providers[0]['name'] = data.attrs[data_attrs['providers']]
        metadata['providers'] = providers
    elif 'institution' in data_attrs_lowercase:
        providers[0]['name'] = data.attrs[data_attrs['institution']]
        metadata['providers'] = providers
    else:
        metadata['providers'] = providers
    if 'links' in data_attrs_lowercase:
        metadata['links'] = data.attrs[data_attrs['links']]
    else:
        metadata['links'] = ''
    x_min = data[x_dim].min().item(0)
    x_max = data[x_dim].max().item(0)
    y_min = data[y_dim].min().item(0)
    y_max = data[y_dim].max().item(0)

    crs_present = False
    coords = list(data.coords)
    if 'spatial_ref' in coords:
        # bands.remove('crs')
        crs_present = True
    # TODO: list bands if more available
    bands = []
    if 'band' in coords:
        bands = list(data['band'].values)
        if len(bands)>0:
            # The JSON decoder does not handle npint types, we need to convert them in advance
            if isinstance(bands[0],np.int8) or \
                isinstance(bands[0],np.int16) or \
                isinstance(bands[0],np.int32) or \
                isinstance(bands[0],np.int64):
                bands = [int(b) for b in bands]
    if crs_present:
        if 'crs_wkt' in data.spatial_ref.attrs:
            transformer = Transformer.from_crs(data.spatial_ref.attrs['crs_wkt'], 'epsg:4326')
            lat_min,lon_min = transformer.transform(x_min,y_min)
            lat_max,lon_max = transformer.transform(x_max,y_max)
            extent = {'spatial': {'bbox': [[lon_min, lat_min, lon_max, lat_max]]}}
            if t_dim is not None:
                t_min = str(data[t_dim].min().values)
                t_max = str(data[t_dim].max().values)
                extent['temporal'] = {'interval': [[t_min,t_max]]}

    metadata['extent'] = extent

    t_dimension = {}
    if t_dim is not None:
        t_dimension = {t_dim: {'type': 'temporal', 'extent':[t_min,t_max]}}

    x_dimension = {x_dim: {'type': 'spatial','axis':'x','extent':[x_min,x_max]}}
    y_dimension = {y_dim: {'type': 'spatial','axis':'y','extent':[y_min,y_max]}}
    if crs_present:
        if 'crs_wkt' in data.spatial_ref.attrs:
            x_dimension[x_dim]['reference_system'] = data.spatial_ref.attrs['crs_wkt']
            y_dimension[y_dim]['reference_system'] = data.spatial_ref.attrs['crs_wkt']

    b_dimension = {}
    if len(bands)>0:
        b_dimension = {'bands': {'type': 'bands', 'values':bands}}

    metadata['cube:dimensions'] = {**t_dimension,**x_dimension,**y_dimension,**b_dimension}

    return metadata

def _get_netcdf_collections(local_collections_path):
    local_collections_netcdfs = glob(local_collections_path + '/*.nc')
    local_collections_list = []
    for local_netcdf in local_collections_netcdfs: 
        metadata = _get_netcdf_metadata(local_netcdf)
        local_collections_list.append(metadata)
    local_collections_dict = {'collections':local_collections_list}
    return local_collections_dict

def _get_geotiff_collections(local_collections_path):
    local_collections_geotiffs = glob(local_collections_path + '/*.tif*')
    local_collections_list = []
    for local_geotiff in local_collections_geotiffs: 
        metadata = _get_geotiff_metadata(local_geotiff)
        local_collections_list.append(metadata)
    local_collections_dict = {'collections':local_collections_list}
    return local_collections_dict

class LocalConnection():
    """
    Connection to no backend, for local processing.
    """

    def __init__(self,local_collections_path):
        """
        Constructor of LocalConnection.

        :param local_collections_path: String path to the folder with the local collections in netCDF or geoTIFF
        """
        self.local_collections_path = local_collections_path.split('file://')[-1]
        
    def list_collections(self) -> List[dict]:
        """
        List basic metadata of all collections provided in the local collections folder.

        .. caution::
        :return: list of dictionaries with basic collection metadata.
        """
        data_nc = _get_netcdf_collections(self.local_collections_path)["collections"]
        data_tif = _get_geotiff_collections(self.local_collections_path)["collections"]
        data = data_nc + data_tif
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
        if '.nc' in Path(collection_id).suffixes:
            data = _get_netcdf_metadata(collection_id)
        elif '.tif' in Path(collection_id).suffixes or '.tiff' in Path(collection_id).suffixes:
            data = _get_geotiff_metadata(collection_id)
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