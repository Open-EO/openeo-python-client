"""
Helpers for data conversions between Python ecosystem data types and openEO data structures.
"""
import collections
import json
from typing import Union

import numpy as np
import pandas
import xarray


class InvalidTimeSeriesException(ValueError):
    pass


def timeseries_json_to_pandas(timeseries: dict, index: str = "date", auto_collapse=True) -> pandas.DataFrame:
    """
    Convert a timeseries JSON object as returned by the `aggregate_polygon` process to a pandas DataFrame object

    This timeseries data has three dimensions in general: date, polygon index and band index.
    One of these will be used as index of the resulting dataframe (as specified by the `index` argument),
    and the other two will be used as multilevel columns.
    When there is just a single polygon or band in play, the dataframe will be simplified
    by removing the corresponding dimension if `auto_collapse` is enabled (on by default).

    :param timeseries: dictionary as returned by `aggregate_polygon` (TODO: is this standardized?)
    :param index: which dimension should be used for the DataFrame index: 'date' or 'polygon'
    :param auto_collapse: whether single band or single polygon cases should be simplified automatically

    :return: pandas DataFrame or Series
    """
    # The input timeseries dictionary is assumed to have this structure:
    #       {dict mapping date -> [list with one item per polygon: [list with one float/None per band or empty list]]}

    # Some quick checks
    if len(timeseries) == 0:
        raise InvalidTimeSeriesException("Empty data set")
    polygon_counts = set(len(polygon_data) for polygon_data in timeseries.values())
    if polygon_counts == {0}:
        raise InvalidTimeSeriesException("No polygon data for each date")
    elif 0 in polygon_counts:
        # TODO: still support this use case?
        raise InvalidTimeSeriesException("No polygon data for some dates ({p})".format(p=polygon_counts))
    elif len(polygon_counts) > 1:
        raise InvalidTimeSeriesException("Inconsistent polygon counts: {p}".format(p=polygon_counts))
    # Count the number of bands in the timeseries, so we can provide a fallback array for missing data
    band_counts = set(len(band_data) for polygon_data in timeseries.values() for band_data in polygon_data)
    if band_counts == {0}:
        raise InvalidTimeSeriesException("Zero bands everywhere")
    band_counts.discard(0)
    if len(band_counts) != 1:
        raise InvalidTimeSeriesException("Inconsistent band counts: {b}".format(b=band_counts))
    band_count = band_counts.pop()
    band_data_fallback = [np.nan] * band_count
    # Load the timeseries data in a pandas Series with multi-index ["date", "polygon", "band"]
    s = pandas.DataFrame.from_records(
        (
            (date, polygon_index, band_index, value)
            for (date, polygon_data) in timeseries.items()
            for polygon_index, band_data in enumerate(polygon_data)
            for band_index, value in enumerate(band_data or band_data_fallback)
        ),
        columns=["date", "polygon", "band", "value"],
        index=["date", "polygon", "band"]
    )["value"].rename(None)
    # TODO convert date to real date index?

    if auto_collapse:
        if s.index.levshape[2] == 1:
            # Single band case
            s.index = s.index.droplevel("band")
        if s.index.levshape[1] == 1:
            # Single polygon case
            s.index = s.index.droplevel("polygon")

    # Reshape as desired
    if index == "date":
        if len(s.index.names) > 1:
            return s.unstack("date").T
        else:
            return s
    elif index == "polygon":
        return s.unstack("polygon").T
    else:
        raise ValueError(index)


def datacube_from_file(filename, fmt='netcdf') -> 'openeo_udf.api.datacube.DataCube':
    """
    Converts source files of different formats into openeo_udf.api.datacube.DataCube in memory
    :param filename: the file on disk
    :param fmt: format to load from
    
    :return: openeo_udf.api.datacube.DataCube
    """ 
    from openeo_udf.api.datacube import DataCube
    if fmt.lower()=='netcdf':
        return DataCube(_load_DataArray_from_NetCDF(filename))
    if fmt.lower()=='json':
        return DataCube(_load_DataArray_from_JSON(filename))


def datacube_to_file(datacube: 'openeo_udf.api.datacube.DataCube', filename, fmt='netcdf'):
    """
    Saves openeo_udf.api.datacube.DataCube to file to different formats
    :param filename: destination file on disk
    :param fmt: format to save as
    
    :return: None
    """ 
    if fmt.lower()=='netcdf':
        _save_DataArray_to_NetCDF(filename, datacube.get_array())
    if fmt.lower()=='json':
        _save_DataArray_to_JSON(filename, datacube.get_array())


def _load_DataArray_from_JSON(filename) -> xarray.DataArray:
    with open(filename) as f:
        # get the deserialized json
        d=json.load(f)
        d['data']=np.array(d['data'],dtype=np.dtype(d['attrs']['dtype']))
        for k,v in d['coords'].items():
            # prepare coordinate 
            d['coords'][k]['data']=np.array(v['data'],dtype=v['attrs']['dtype'])
            # remove dtype and shape, because that is included for helping the user
            if d['coords'][k].get('attrs',None) is not None:
                d['coords'][k]['attrs'].pop('dtype',None)
                d['coords'][k]['attrs'].pop('shape',None)
        
        # remove dtype and shape, because that is included for helping the user
        if d.get('attrs',None) is not None:
            d['attrs'].pop('dtype',None)
            d['attrs'].pop('shape',None)
        # vonvert to xarray
        r=xarray.DataArray.from_dict(d)
        del d
    # build dimension list in proper order
    dims=list(filter(lambda i: i!='t' and i!='bands' and i!='x' and i!='y',r.dims))
    if 't' in r.dims: dims+=['t']
    if 'bands' in r.dims: dims+=['bands']
    if 'x' in r.dims: dims+=['x']
    if 'y' in r.dims: dims+=['y']
    # return the resulting data array
    return r.transpose(*dims)


def _load_DataArray_from_NetCDF(filename) -> xarray.DataArray:
    # load the dataset and convert to data array
    ds=xarray.open_dataset(filename, engine='h5netcdf')
    r=ds.to_array(dim='bands')
    # build dimension list in proper order
    dims=list(filter(lambda i: i!='t' and i!='bands' and i!='x' and i!='y',r.dims))
    if 't' in r.dims: dims+=['t']
    if 'bands' in r.dims: dims+=['bands']
    if 'x' in r.dims: dims+=['x']
    if 'y' in r.dims: dims+=['y']
    # return the resulting data array
    return r.transpose(*dims)


def _save_DataArray_to_JSON(filename, array: xarray.DataArray):
    # to deserialized json
    jsonarray=array.to_dict()
    # add attributes that needed for re-creating xarray from json
    jsonarray['attrs']['dtype']=str(array.values.dtype)
    jsonarray['attrs']['shape']=list(array.values.shape)
    for i in array.coords.values():
        jsonarray['coords'][i.name]['attrs']['dtype']=str(i.dtype)
        jsonarray['coords'][i.name]['attrs']['shape']=list(i.shape)
    # custom print so resulting json file is humanly easy to read
    with open(filename,'w') as f:
        def custom_print(data_structure, indent=1):
            f.write("{\n")
            needs_comma=False
            for key, value in data_structure.items():
                if needs_comma: 
                    f.write(',\n')
                needs_comma=True
                f.write('  '*indent+json.dumps(key)+':')
                if isinstance(value, dict): 
                    custom_print(value, indent+1)
                else: 
                    json.dump(value,f,default=str,separators=(',',':'))
            f.write('\n'+'  '*(indent-1)+"}")
            
        custom_print(jsonarray)


def _save_DataArray_to_NetCDF(filename, array: xarray.DataArray):
    # temp reference to avoid modifying the original array
    result=array
    # rearrange in a basic way because older xarray versions have a bug and ellipsis don't work in xarray.transpose()
    if result.dims[-2]=='x' and result.dims[-1]=='y':
        l=list(result.dims[:-2])
        result=result.transpose(*(l+['y','x']))
    # turn it into a dataset where each band becomes a variable
    if not 'bands' in result.dims:
        result=result.expand_dims(dim=collections.OrderedDict({'bands':['band_0']}))
    else:
        if not 'bands' in result.coords:
            labels=['band_'+str(i) for i in range(result.shape[result.dims.index('bands')])]
            result=result.assign_coords(bands=labels)
    result=result.to_dataset('bands')
    result.to_netcdf(filename, engine='h5netcdf')


def datacube_plot(
        datacube:'openeo_udf.api.datacube.DataCube',
        title:str=None, 
        limits=None,
        show_bandnames:bool=True,
        show_dates:bool=True,
        fontsize:float=10.,
        oversample:int=1,
        cmap:Union[str,'matplotlib.colors.ColorMap']='RdYlBu_r', 
        cbartext:str=None,
        to_file:str=None,
        to_show:bool=True
    ):
    """
    Plots an openeo_udf.api.datacube.DataCube 
    :param datacube: data to plot
    :param title: title text drawn in the top left corner (default: nothing)
    :param limits: range of the contour plot as a tuple(min,max) (default: None, in which case the min/max is computed from the data)
    :param show_bandnames: whether to plot the column names (default: True)
    :param show_dates: whether to show the dates for each row (defaukt: True)
    :param fontsize: font size in pixels (default: 10)
    :param oversample: one value is plotted into oversample x oversample number of pixels (default: 1 which means each value is plotted as a single pixel)
    :param cmap: built-in matplotlib color map name or ColorMap object (default: RdYlBu_r which is a blue-yellow-red rainbow)
    :param cbartext: text on top of the legend (default: nothing)
    :param to_file: filename to save the image to (default: None, which means no file is generated)
    :param to_show: whether to show the image in a matplotlib window (default: True)
    
    :return: None
    """ 

    from matplotlib import pyplot
    
    data=datacube.get_array()
    if limits is None:
        vmin=data.min()
        vmax=data.max()
    else: 
        vmin=limits[0]
        vmax=limits[1]
    nrow=data.shape[0]
    ncol=data.shape[1]
    data=data.transpose('t','bands','y','x')
    dpi=100
    xres=len(data.x)/dpi
    yres=len(data.y)/dpi
    fs=fontsize/oversample
    frame=0.33

    fig = pyplot.figure(figsize=((ncol+frame)*xres*1.1,(nrow+frame)*yres),dpi=int(dpi*oversample)) 
    gs = pyplot.GridSpec(nrow,ncol,wspace=0.,hspace=0.,top=nrow/(nrow+frame),bottom=0.,left=frame/(ncol+frame),right=1.) 
     
    for i in range(nrow):
        for j in range(ncol):
            im = data[i,j]
            ax= pyplot.subplot(gs[i,j])
            ax.set_axis_off()
            img=ax.imshow(im[::-1,:],vmin=vmin,vmax=vmax,cmap=cmap)
            ax.set_xticklabels([])
            ax.set_yticklabels([])
            if show_bandnames:
                if i==0: ax.text(0.5,1.08, data.bands.values[j]+" ("+str(data.dtype)+")", size=fs, va="center", ha="center", transform=ax.transAxes)
            if show_dates:
                if j==0: ax.text(-0.08,0.5, data.t.dt.strftime("%Y-%m-%d").values[i], size=fs, va="center", ha="center", rotation=90,  transform=ax.transAxes)

    if title is not None:
        fig.text(0.,1.,title.split('/')[-1], size=fs, va="top", ha="left",weight='bold')

    cbar_ax = fig.add_axes([0.01, 0.1, 0.04, 0.5])
    if cbartext is not None:
        fig.text(0.06,0.62,cbartext, size=fs, va="bottom", ha="center")
    cbar=fig.colorbar(img, cax=cbar_ax)
    cbar.ax.tick_params(labelsize=fs)
    cbar.outline.set_visible(False)
    cbar.ax.tick_params(size=0)
    cbar.ax.yaxis.set_tick_params(pad=0)
    
    if to_file is not None: pyplot.savefig(to_file)
    if to_show: pyplot.show()

    pyplot.close()
