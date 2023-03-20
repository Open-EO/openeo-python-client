import pytest
import xarray as xr
import numpy as np
import pandas as pd

try:
    from openeo.local import LocalConnection
except ImportError:
    LocalConnection = None


@pytest.mark.skipif(
    not LocalConnection, reason="environment does not support localprocessing"
)
def test_local_collection_metadata(tmp_path_factory):
    sample_netcdf = create_local_data(tmp_path_factory,2,2,2,'netcdf')
    sample_geotiff = create_local_data(tmp_path_factory,2,2,2,'tiff')
    local_conn = LocalConnection(sample_netcdf.as_posix())
    assert len(local_conn.list_collections()) == 1
    local_conn = LocalConnection([sample_netcdf.as_posix(),sample_geotiff.as_posix()])
    assert len(local_conn.list_collections()) == 2

def create_local_data(tmp_path_factory,lat_size,lon_size,t_size,file_format):
    np.random.seed(0)
    lon = np.linspace(10.5,11.5,lon_size)
    lat = np.linspace(46.0,47.0,lat_size)
    time = pd.date_range('2014-09-06', periods=t_size)
    reference_time = pd.Timestamp('2014-09-05')

    if file_format.lower() in ['nc','netcdf']:
        temperature = 15 + 8 * np.random.randn(lat_size, lon_size, t_size)
        precipitation = 10 * np.random.rand(lat_size, lon_size, t_size)
        ds = xr.Dataset(
            data_vars=dict(
                temperature=(['x', 'y', 'time'], temperature),
                precipitation=(['x', 'y', 'time'], precipitation),
            ),
            coords=dict(
                lon=(['x'], lon),
                lat=(['y'], lat),
                time=time,
                reference_time=reference_time,
            ),
            attrs=dict(description='Weather related data.'),
        )
        d = tmp_path_factory.mktemp('sample_netcdf')
        ds.to_netcdf(d / 'sample_data.nc')
    elif file_format.lower() in ['tif','tiff','geotiff']:
        temperature = 15 + 8 * np.random.randn(lat_size, lon_size)
        precipitation = 10 * np.random.rand(lat_size, lon_size)
        ds = xr.Dataset(
            data_vars=dict(
                temperature=(['x', 'y'], temperature),
                precipitation=(['x', 'y'], precipitation),
            ),
            coords=dict(
                lon=(['x'], lon),
                lat=(['y'], lat),
            ),
            attrs=dict(description='Weather related data.'),
        )
        d = tmp_path_factory.mktemp('sample_geotiff')
        ds.to_array().transpose('variable', 'y', 'x').rio.to_raster(d / 'sample_data.tiff')

    return d
