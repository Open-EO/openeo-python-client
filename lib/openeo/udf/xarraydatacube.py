"""

"""

# Note: this module was initially developed under the ``openeo-udf`` project (https://github.com/Open-EO/openeo-udf)

from __future__ import annotations

import collections
import json
import typing
from pathlib import Path
from typing import Optional, Union

import numpy
import xarray

from openeo.udf import OpenEoUdfException
from openeo.util import deep_get, dict_no_none

if typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime).
    import matplotlib.colors


class XarrayDataCube:
    """
    This is a thin wrapper around :py:class:`xarray.DataArray`
    providing a basic "DataCube" interface for openEO UDF usage around multi-dimensional data.
    """

    # TODO #472 This class, just wrapping an array.DataArray, seems to make things more complicated/confusing than necessary.

    def __init__(self, array: xarray.DataArray):
        if not isinstance(array, xarray.DataArray):
            raise OpenEoUdfException("Argument data must be of type xarray.DataArray")
        self._array = array

    def __repr__(self):
        return f"<{type(self).__name__} shape:{self._array.shape}>"

    def get_array(self) -> xarray.DataArray:
        """
        Get the :py:class:`xarray.DataArray` that contains the data and dimension definition
        """
        return self._array

    array = property(fget=get_array)

    @property
    def id(self):
        return self._array.name

    def to_dict(self) -> dict:
        """
        Convert this hypercube into a dictionary that can be converted into
        a valid JSON representation

        >>> example = {
        ...     "id": "test_data",
        ...     "data": [
        ...         [[0.0, 0.1], [0.2, 0.3]],
        ...         [[0.0, 0.1], [0.2, 0.3]],
        ...     ],
        ...     "dimension": [
        ...         {"name": "time", "coordinates": ["2001-01-01", "2001-01-02"]},
        ...         {"name": "X", "coordinates": [50.0, 60.0]},
        ...         {"name": "Y"},
        ...     ],
        ... }
        """
        xd = self._array.to_dict()
        return dict_no_none({
            "id": xd.get("name"),
            "data": xd.get("data"),
            "description": deep_get(xd, "attrs", "description", default=None),
            "dimensions": [
                dict_no_none(
                    name=dim,
                    coordinates=deep_get(xd, "coords", dim, "data", default=None)
                )
                for dim in xd.get("dims", [])
            ]
        })

    @classmethod
    def from_dict(cls, xdc_dict: dict) -> XarrayDataCube:
        """
        Create a :py:class:`XarrayDataCube` from a Python dictionary that was created from
        the JSON definition of the data cube

        :param data: The dictionary that contains the data cube definition
        """

        if "data" not in xdc_dict:
            raise OpenEoUdfException("Missing data in dictionary")

        data = numpy.asarray(xdc_dict["data"])

        if "dimensions" in xdc_dict:
            dims = [dim["name"] for dim in xdc_dict["dimensions"]]
            coords = {dim["name"]: dim["coordinates"] for dim in xdc_dict["dimensions"] if "coordinates" in dim}
        else:
            dims = None
            coords = None

        x = xarray.DataArray(data, dims=dims, coords=coords, name=xdc_dict.get("id"))

        if "description" in xdc_dict:
            x.attrs["description"] = xdc_dict["description"]

        return cls(array=x)

    @staticmethod
    def _guess_format(path: Union[str, Path]) -> str:
        """Guess file format from file name."""
        suffix = Path(path).suffix.lower()
        if suffix in [".nc", ".netcdf"]:
            return "netcdf"
        elif suffix in [".json"]:
            return "json"
        else:
            raise ValueError("Can not guess format of {p}".format(p=path))

    @classmethod
    def from_file(cls, path: Union[str, Path], fmt=None, **kwargs) -> XarrayDataCube:
        """
        Load data file as :py:class:`XarrayDataCube` in memory

        :param path: the file on disk
        :param fmt: format to load from, e.g. "netcdf" or "json"
            (will be auto-detected when not specified)

        :return: loaded data cube
        """
        fmt = fmt or cls._guess_format(path)
        if fmt.lower() == 'netcdf':
            return cls(array=XarrayIO.from_netcdf_file(path=path, **kwargs))
        elif fmt.lower() == 'json':
            return cls(array=XarrayIO.from_json_file(path=path))
        else:
            raise ValueError("invalid format {f}".format(f=fmt))

    def save_to_file(self, path: Union[str, Path], fmt=None, **kwargs):
        """
        Store :py:class:`XarrayDataCube` to file

        :param path: destination file on disk
        :param fmt: format to save as, e.g. "netcdf" or "json"
            (will be auto-detected when not specified)
        """
        fmt = fmt or self._guess_format(path)
        if fmt.lower() == 'netcdf':
            XarrayIO.to_netcdf_file(array=self.get_array(), path=path, **kwargs)
        elif fmt.lower() == 'json':
            XarrayIO.to_json_file(array=self.get_array(), path=path)
        else:
            raise ValueError(fmt)

    def plot(
            self,
            title: str = None,
            limits=None,
            show_bandnames: bool = True,
            show_dates: bool = True,
            show_axeslabels: bool = False,
            fontsize: float = 10.,
            oversample: float = 1,
            cmap: Union[str, 'matplotlib.colors.Colormap'] = 'RdYlBu_r',
            cbartext: str = None,
            to_file: str = None,
            to_show: bool = True
    ):
        """
        Visualize a :py:class:`XarrayDataCube` with matplotlib

        :param datacube: data to plot
        :param title: title text drawn in the top left corner (default: nothing)
        :param limits: range of the contour plot as a tuple(min,max) (default: None, in which case the min/max is computed from the data)
        :param show_bandnames: whether to plot the column names (default: True)
        :param show_dates: whether to show the dates for each row (default: True)
        :param show_axeslabels: whether to show the labels on the axes (default: False)
        :param fontsize: font size in pixels (default: 10)
        :param oversample: one value is plotted into oversample x oversample number of pixels (default: 1 which means each value is plotted as a single pixel)
        :param cmap: built-in matplotlib color map name or ColorMap object (default: RdYlBu_r which is a blue-yellow-red rainbow)
        :param cbartext: text on top of the legend (default: nothing)
        :param to_file: filename to save the image to (default: None, which means no file is generated)
        :param to_show: whether to show the image in a matplotlib window (default: True)

        :return: None
        """
        from matplotlib import pyplot

        data = self.get_array()
        if limits is None:
            vmin = data.min()
            vmax = data.max()
        else:
            vmin = limits[0]
            vmax = limits[1]

        # fill bands and t if missing
        if 'bands' not in data.dims:
            data = data.expand_dims(dim={'bands': ['band0']})
        if 't' not in data.dims:
            data = data.expand_dims(dim={'t': [numpy.datetime64('today')]})
        if 'bands' not in data.coords:
            data['bands'] = ['band0']
        if 't' not in data.coords:
            data['t'] = [numpy.datetime64('today')]

        # align with plot
        data = data.transpose('t', 'bands', 'y', 'x')
        dpi = 100
        xres = len(data.x) / dpi
        yres = len(data.y) / dpi
        fs = fontsize / oversample
        frame = 0.33

        nrow = data.shape[0]
        ncol = data.shape[1]

        fig = pyplot.figure(figsize=((ncol + frame) * xres * 1.1, (nrow + frame) * yres), dpi=int(dpi * oversample))
        gs = pyplot.GridSpec(nrow, ncol, wspace=0., hspace=0., top=nrow / (nrow + frame), bottom=0.,
                             left=frame / (ncol + frame), right=1.)

        xmin = data.x.min()
        xmax = data.x.max()
        ymin = data.y.min()
        ymax = data.y.max()

        # flip around if incorrect, this is in harmony with origin='lower'
        if (data.x[0] > data.x[-1]):
            data = data.reindex(x=list(reversed(data.x)))
        if (data.y[0] > data.y[-1]):
            data = data.reindex(y=list(reversed(data.y)))

        extent = (data.x[0], data.x[-1], data.y[0], data.y[-1])

        for i in range(nrow):
            for j in range(ncol):
                im = data[i, j]
                ax = pyplot.subplot(gs[i, j])
                ax.set_xlim(xmin, xmax)
                ax.set_ylim(ymin, ymax)
                img = ax.imshow(im, vmin=vmin, vmax=vmax, cmap=cmap, origin='lower', extent=extent)
                ax.xaxis.set_tick_params(labelsize=fs)
                ax.yaxis.set_tick_params(labelsize=fs)
                if not show_axeslabels:
                    ax.set_axis_off()
                    ax.set_xticklabels([])
                    ax.set_yticklabels([])
                if show_bandnames:
                    if i == 0: ax.text(0.5, 1.08, data.bands.values[j] + " (" + str(data.dtype) + ")", size=fs,
                                       va="center",
                                       ha="center", transform=ax.transAxes)
                if show_dates:
                    if j == 0: ax.text(-0.08, 0.5, data.t.dt.strftime("%Y-%m-%d").values[i], size=fs, va="center",
                                       ha="center", rotation=90, transform=ax.transAxes)

        if title is not None:
            fig.text(0., 1., title.split('/')[-1], size=fs, va="top", ha="left", weight='bold')

        cbar_ax = fig.add_axes([0.01, 0.1, 0.04, 0.5])
        if cbartext is not None:
            fig.text(0.06, 0.62, cbartext, size=fs, va="bottom", ha="center")
        cbar = fig.colorbar(img, cax=cbar_ax)
        cbar.ax.tick_params(labelsize=fs)
        cbar.outline.set_visible(False)
        cbar.ax.tick_params(size=0)
        cbar.ax.yaxis.set_tick_params(pad=0)

        if to_file is not None:
            pyplot.savefig(str(to_file))
        if to_show:
            pyplot.show()

        pyplot.close()


class XarrayIO:
    """
    Helpers to load/store :py:cass:`xarray.DataArray` objects,
    with some conventions about expected dimensions/bands
    """

    @classmethod
    def from_json_file(cls, path: Union[str, Path]) -> xarray.DataArray:
        with Path(path).open() as f:
            return cls.from_json(json.load(f))

    @classmethod
    def from_json(cls, d: dict) -> xarray.DataArray:
        d['data'] = numpy.array(d['data'], dtype=numpy.dtype(d['attrs']['dtype']))
        for k, v in d['coords'].items():
            # prepare coordinate
            d['coords'][k]['data'] = numpy.array(v['data'], dtype=v['attrs']['dtype'])
            # remove dtype and shape, because that is included for helping the user
            if d['coords'][k].get('attrs', None) is not None:
                d['coords'][k]['attrs'].pop('dtype', None)
                d['coords'][k]['attrs'].pop('shape', None)

        # remove dtype and shape, because that is included for helping the user
        if d.get('attrs', None) is not None:
            d['attrs'].pop('dtype', None)
            d['attrs'].pop('shape', None)
        # convert to xarray
        r = xarray.DataArray.from_dict(d)

        # build dimension list in proper order
        dims = list(filter(lambda i: i != 't' and i != 'bands' and i != 'x' and i != 'y', r.dims))
        if 't' in r.dims: dims += ['t']
        if 'bands' in r.dims: dims += ['bands']
        if 'x' in r.dims: dims += ['x']
        if 'y' in r.dims: dims += ['y']
        # return the resulting data array
        return r.transpose(*dims)

    @classmethod
    def from_netcdf_file(cls, path: Union[str, Path], engine: Optional[str] = None) -> xarray.DataArray:
        # load the dataset and convert to data array
        ds = xarray.open_dataset(path, engine=engine)

        # Skip non-numerical variables (like "crs")
        band_vars = [k for k, v in ds.data_vars.items() if v.dtype.kind in {"b", "i", "u", "f"} and len(v.dims) > 0]
        ds = ds[band_vars]

        r = ds.to_array(dim='bands')

        # Reorder dims to proper order (t-bands-x-y at the end)
        expected_order = ("t", "bands", "x", "y")
        dims = [d for d in r.dims if d not in expected_order] + [d for d in expected_order if d in r.dims]

        return r.transpose(*dims)

    @classmethod
    def to_json_file(cls, array: xarray.DataArray, path: Union[str, Path]):
        # to deserialized json
        jsonarray = array.to_dict()
        # add attributes that needed for re-creating xarray from json
        jsonarray['attrs']['dtype'] = str(array.values.dtype)
        jsonarray['attrs']['shape'] = list(array.values.shape)
        for i in array.coords.values():
            jsonarray['coords'][i.name]['attrs']['dtype'] = str(i.dtype)
            jsonarray['coords'][i.name]['attrs']['shape'] = list(i.shape)
        # custom print so resulting json file is humanly easy to read
        # TODO: make this human friendly JSON format optional and allow compact JSON too.
        with Path(path).open("w", encoding="utf-8") as f:
            def custom_print(data_structure, indent=1):
                f.write("{\n")
                needs_comma = False
                for key, value in data_structure.items():
                    if needs_comma:
                        f.write(',\n')
                    needs_comma = True
                    f.write('  ' * indent + json.dumps(key) + ':')
                    if isinstance(value, dict):
                        custom_print(value, indent + 1)
                    else:
                        json.dump(value, f, default=str, separators=(',', ':'))
                f.write('\n' + '  ' * (indent - 1) + "}")

            custom_print(jsonarray)

    @classmethod
    def to_netcdf_file(cls, array: xarray.DataArray, path: Union[str, Path], engine: Optional[str] = None):
        # temp reference to avoid modifying the original array
        result = array
        # rearrange in a basic way because older xarray versions have a bug and ellipsis don't work in xarray.transpose()
        if result.dims[-2] == 'x' and result.dims[-1] == 'y':
            l = list(result.dims[:-2])
            result = result.transpose(*(l + ['y', 'x']))
        # turn it into a dataset where each band becomes a variable
        if not 'bands' in result.dims:
            result = result.expand_dims(dim=collections.OrderedDict({'bands': ['band_0']}))
        else:
            if not 'bands' in result.coords:
                labels = ['band_' + str(i) for i in range(result.shape[result.dims.index('bands')])]
                result = result.assign_coords(bands=labels)
        result = result.to_dataset('bands')
        result.to_netcdf(path, engine=engine)
