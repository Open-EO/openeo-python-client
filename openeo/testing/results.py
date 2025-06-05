"""
Assert functions for comparing actual (batch job) results against expected reference data.
"""

import json
import logging
import tempfile
from pathlib import Path
from typing import List, Optional, Union

import numpy as np
import xarray
import xarray.testing
from xarray import DataArray

from openeo.rest.job import DEFAULT_JOB_RESULTS_FILENAME, BatchJob, JobResults
from openeo.util import repr_truncate

_log = logging.getLogger(__name__)

_DEFAULT_RTOL = 1e-6
_DEFAULT_ATOL = 1e-6
_DEFAULT_PIXELTOL = 0.0

# https://paulbourke.net/dataformats/asciiart
DEFAULT_GRAYSCALE_70_CHARACTERS = r"$@B%8&WM#*oahkbdpqwmZO0QLCJUYXzcvunxrjft/\|()1{}[]?-_+~<>i!lI;:,\"^`'. "[::-1]
DEFAULT_GRAYSCALE_10_CHARACTERS = " .:-=+*#%@"


def _load_xarray_netcdf(path: Union[str, Path], **kwargs) -> xarray.Dataset:
    """
    Load a netCDF file as Xarray Dataset
    """
    _log.debug(f"_load_xarray_netcdf: {path!r}")
    return xarray.load_dataset(path, **kwargs)


def _load_rioxarray_geotiff(path: Union[str, Path], **kwargs) -> xarray.DataArray:
    """
    Load a GeoTIFF file as Xarray DataArray (using `rioxarray` extension).
    """
    _log.debug(f"_load_rioxarray_geotiff: {path!r}")
    try:
        import rioxarray
    except ImportError as e:
        raise ImportError("This feature requires 'rioxarray` as optional dependency.") from e
    return rioxarray.open_rasterio(path, **kwargs)


def _load_xarray(path: Union[str, Path], **kwargs) -> Union[xarray.Dataset, xarray.DataArray]:
    """
    Generically load a netCDF/GeoTIFF file as Xarray Dataset/DataArray.
    """
    path = Path(path)
    if path.suffix.lower() in {".nc", ".netcdf"}:
        return _load_xarray_netcdf(path, **kwargs)
    elif path.suffix.lower() in {".tif", ".tiff", ".gtiff", ".geotiff"}:
        return _load_rioxarray_geotiff(path, **kwargs)
    raise ValueError(f"Unsupported file type: {path}")


def _load_json(path: Union[str, Path]) -> dict:
    """
    Load a JSON file.
    """
    with Path(path).open("r", encoding="utf-8") as f:
        return json.load(f)


def _as_xarray_dataset(data: Union[str, Path, xarray.Dataset]) -> xarray.Dataset:
    """
    Get data as Xarray Dataset (loading from file if needed).
    """
    if isinstance(data, (str, Path)):
        data = _load_xarray(data)
    # TODO auto-convert DataArray to Dataset?
    if not isinstance(data, xarray.Dataset):
        raise ValueError(f"Unsupported type: {type(data)}")
    return data


def _as_xarray_dataarray(data: Union[str, Path, xarray.DataArray]) -> xarray.DataArray:
    """
    Convert a path to a NetCDF/GeoTIFF file to an Xarray DataArray.

    :param data: path to a NetCDF/GeoTIFF file or Xarray DataArray
    :return: Xarray DataArray
    """
    if isinstance(data, (str, Path)):
        data = _load_xarray(data)
    # TODO: auto-convert Dataset to DataArray?
    if not isinstance(data, xarray.DataArray):
        raise ValueError(f"Unsupported type: {type(data)}")
    return data


def _ascii_art(
    diff_data: DataArray,
    *,
    max_width: int = 60,
    y_vs_x_aspect_ratio=2.5,
    grayscale_characters: str = DEFAULT_GRAYSCALE_70_CHARACTERS,
) -> str:
    max_grayscale_idx = len(grayscale_characters) - 1
    x_scale: int = max(1, int(diff_data.sizes["x"] / max_width))
    y_scale: int = max(1, int(diff_data.sizes["y"] / (max_width / y_vs_x_aspect_ratio)))
    data_max = diff_data.max().item()
    if data_max == 0:
        data_max = 1
    coarsened = diff_data.coarsen(dim={"x": x_scale, "y": y_scale}, boundary="pad").mean()
    coarsened = coarsened.transpose("y", "x", ...)
    top = "┌" + "─" * coarsened.sizes["x"] + "┐\n"
    bottom = "\n└" + "─" * coarsened.sizes["x"] + "┘"

    def _pixel_char(v) -> str:
        i = 0 if np.isnan(v) else int(v * max_grayscale_idx / data_max)
        if v > 0 and i == 0:
            i = 1  # don't show a blank for a difference above the threshold
        else:
            i = min(max_grayscale_idx, i)
        return grayscale_characters[i]

    return top + "\n".join(["│" + "".join([_pixel_char(v) for v in row]) + "│" for row in coarsened]) + bottom


def _compare_xarray_dataarray_xy(
    actual: Union[xarray.DataArray, str, Path],
    expected: Union[xarray.DataArray, str, Path],
    *,
    rtol: float = _DEFAULT_RTOL,
    atol: float = _DEFAULT_ATOL,
    name: str = None,
) -> List[str]:
    """
    Additional compare for two compatible spatial xarray DataArrays with tolerance (rtol, atol)
    :return: list of issues (empty if no issues)
    """
    issues = []

    actual_as_float = actual.astype(dtype=float)
    expected_as_float = expected.astype(dtype=float)

    threshold = abs(expected_as_float * rtol) + atol
    diff_exact = abs(expected_as_float - actual_as_float)
    diff_mask = diff_exact > threshold
    diff_lenient = diff_exact.where(diff_mask)

    non_x_y_dims = list(set(expected_as_float.dims) - {"x", "y"})
    value_mapping = dict(map(lambda d: (d, expected_as_float[d].data), non_x_y_dims))
    shape = tuple([len(value_mapping[x]) for x in non_x_y_dims])

    for shape_index, v in np.ndenumerate(np.ndarray(shape)):
        indexers = {}
        for index, value_index in enumerate(shape_index):
            indexers[non_x_y_dims[index]] = value_mapping[non_x_y_dims[index]][value_index]
        diff_data = diff_lenient.sel(indexers=indexers)
        total_pixel_count = expected_as_float.sel(indexers).count().item()
        diff_pixel_count = diff_data.count().item()

        if diff_pixel_count > 0:
            diff_pixel_percentage = round(diff_pixel_count * 100 / total_pixel_count, 1)
            diff_mean = round(diff_data.mean().item(), 2)
            diff_var = round(diff_data.var().item(), 2)

            key = name + ": " if name else ""
            key += ",".join([f"{k} {str(v1)}" for k, v1 in indexers.items()])
            issues.append(
                f"{key}: value difference exceeds tolerance (rtol {rtol}, atol {atol}), min:{diff_data.min().data}, max: {diff_data.max().data}, mean: {diff_mean}, var: {diff_var}"
            )

            _log.warning(f"Difference (ascii art) for {key}:\n{_ascii_art(diff_data)}")

            coord_grid = np.meshgrid(diff_data.coords["x"], diff_data.coords["y"])
            mask = diff_data.notnull()
            if mask.dims[0] != "y":
                mask = mask.transpose()
            x_coords = coord_grid[0][mask]
            y_coords = coord_grid[1][mask]

            diff_bbox = ((x_coords.min().item(), y_coords.min().item()), (x_coords.max().item(), y_coords.max().item()))
            diff_area = (x_coords.max() - x_coords.min()) * (y_coords.max() - y_coords.min())
            total_area = abs(
                (diff_data.coords["y"][-1].data - diff_data.coords["y"][0].data)
                * (diff_data.coords["x"][-1].data - diff_data.coords["x"][0].data)
            )
            area_percentage = round(diff_area * 100 / total_area, 1)
            issues.append(
                f"{key}: differing pixels: {diff_pixel_count}/{total_pixel_count} ({diff_pixel_percentage}%), bbox {diff_bbox} - {area_percentage}% of the area"
            )
    return issues


def _compare_xarray_dataarray(
    actual: Union[xarray.DataArray, str, Path],
    expected: Union[xarray.DataArray, str, Path],
    *,
    rtol: float = _DEFAULT_RTOL,
    atol: float = _DEFAULT_ATOL,
    pixel_tolerance: float = _DEFAULT_PIXELTOL,
    name: str = None,
) -> List[str]:
    """
    Compare two xarray DataArrays with tolerance and report mismatch issues (as strings)

    Checks that are done (with tolerance):
    - (optional) Check fraction of mismatching pixels (difference exceeding some tolerance).
      If fraction is below a given threshold, ignore these mismatches in subsequent comparisons.
      If fraction is above the threshold, report this issue.
    - Compare actual and expected data with `xarray.testing.assert_allclose` and specified tolerances.

    :return: list of issues (empty if no issues)
    """
    # TODO: make this a public function?
    # TODO: option for nodata fill value?
    # TODO: option to include data type check?
    # TODO: option to cast to some data type (or even rescale) before comparison?
    # TODO: also compare attributes of the DataArray?
    actual = _as_xarray_dataarray(actual)
    expected = _as_xarray_dataarray(expected)
    issues = []

    # `xarray.testing.assert_allclose` currently does not always
    # provide detailed information about shape/dimension mismatches
    # so we enrich the issue listing with some more details
    if actual.dims != expected.dims:
        issues.append(f"Dimension mismatch: {actual.dims} != {expected.dims}")
    for dim in sorted(set(expected.dims).intersection(actual.dims)):
        acs = actual.coords[dim].values
        ecs = expected.coords[dim].values
        if not (acs.shape == ecs.shape and (acs == ecs).all()):
            issues.append(f"Coordinates mismatch for dimension {dim!r}: {acs} != {ecs}")
    if actual.shape != expected.shape:
        issues.append(f"Shape mismatch: {actual.shape} != {expected.shape}")
    compatible = len(issues) == 0
    try:
        if pixel_tolerance and compatible:
            threshold = abs(expected * rtol) + atol
            bad_pixels = abs(actual * 1.0 - expected * 1.0) > threshold
            percentage_bad_pixels = bad_pixels.mean().item() * 100
            assert (
                percentage_bad_pixels <= pixel_tolerance
            ), f"Fraction significantly differing pixels: {percentage_bad_pixels}% > {pixel_tolerance}%"
            xarray.testing.assert_allclose(
                a=actual.where(~bad_pixels), b=expected.where(~bad_pixels), rtol=rtol, atol=atol
            )
        else:
            xarray.testing.assert_allclose(a=actual, b=expected, rtol=rtol, atol=atol)
    except AssertionError as e:
        issues.append(str(e).strip())
        if compatible and {"x", "y"} <= set(expected.dims):
            issues.extend(
                _compare_xarray_dataarray_xy(actual=actual, expected=expected, rtol=rtol, atol=atol, name=name)
            )
    return issues


def assert_xarray_dataarray_allclose(
    actual: Union[xarray.DataArray, str, Path],
    expected: Union[xarray.DataArray, str, Path],
    *,
    rtol: float = _DEFAULT_RTOL,
    atol: float = _DEFAULT_ATOL,
):
    """
    Assert that two Xarray ``DataArray`` instances are equal (with tolerance).

    :param actual: actual data, provided as Xarray DataArray object or path to NetCDF/GeoTIFF file.
    :param expected: expected or reference data, provided as Xarray DataArray object or path to NetCDF/GeoTIFF file.
    :param rtol: relative tolerance
    :param atol: absolute tolerance
    :raises AssertionError: if not equal within the given tolerance

    .. versionadded:: 0.31.0

    .. warning::
        This function is experimental and subject to change.
    """
    issues = _compare_xarray_dataarray(actual=actual, expected=expected, rtol=rtol, atol=atol)
    if issues:
        raise AssertionError("\n".join(issues))


def _compare_xarray_datasets(
    actual: Union[xarray.Dataset, str, Path],
    expected: Union[xarray.Dataset, str, Path],
    *,
    rtol: float = _DEFAULT_RTOL,
    atol: float = _DEFAULT_ATOL,
    pixel_tolerance: float = _DEFAULT_PIXELTOL,
) -> List[str]:
    """
    Compare two xarray ``DataSet``s with tolerance and report mismatch issues (as strings)

    :return: list of issues (empty if no issues)
    """
    # TODO: make this a public function?
    actual = _as_xarray_dataset(actual)
    expected = _as_xarray_dataset(expected)

    all_issues = []
    actual_vars = set(actual.data_vars)
    expected_vars = set(expected.data_vars)
    _log.debug(f"_compare_xarray_datasets: actual_vars={actual_vars!r} expected_vars={expected_vars!r}")
    if actual_vars != expected_vars:
        all_issues.append(f"Xarray DataSet variables mismatch: {actual_vars} != {expected_vars}")
    for var in expected_vars.intersection(actual_vars):
        _log.debug(f"_compare_xarray_datasets: comparing variable {var!r}")
        issues = _compare_xarray_dataarray(
            actual[var], expected[var], rtol=rtol, atol=atol, pixel_tolerance=pixel_tolerance, name=var
        )
        if issues:
            all_issues.append(f"Issues for variable {var!r}:")
            all_issues.extend(issues)
    return all_issues


def assert_xarray_dataset_allclose(
    actual: Union[xarray.Dataset, str, Path],
    expected: Union[xarray.Dataset, str, Path],
    *,
    rtol: float = _DEFAULT_RTOL,
    atol: float = _DEFAULT_ATOL,
):
    """
    Assert that two Xarray ``DataSet`` instances are equal (with tolerance).

    :param actual: actual data, provided as Xarray Dataset object or path to NetCDF/GeoTIFF file
    :param expected: expected or reference data, provided as Xarray Dataset object or path to NetCDF/GeoTIFF file.
    :param rtol: relative tolerance
    :param atol: absolute tolerance
    :raises AssertionError: if not equal within the given tolerance

    .. versionadded:: 0.31.0

    .. warning::
        This function is experimental and subject to change.
    """
    issues = _compare_xarray_datasets(actual=actual, expected=expected, rtol=rtol, atol=atol)
    if issues:
        raise AssertionError("\n".join(issues))


def assert_xarray_allclose(
    actual: Union[xarray.Dataset, xarray.DataArray, str, Path],
    expected: Union[xarray.Dataset, xarray.DataArray, str, Path],
    *,
    rtol: float = _DEFAULT_RTOL,
    atol: float = _DEFAULT_ATOL,
):
    """
    Assert that two Xarray ``DataSet`` or ``DataArray`` instances are equal (with tolerance).

    :param actual: actual data, provided as Xarray object or path to NetCDF/GeoTIFF file.
    :param expected: expected or reference data, provided as Xarray object or path to NetCDF/GeoTIFF file.
    :param rtol: relative tolerance
    :param atol: absolute tolerance
    :raises AssertionError: if not equal within the given tolerance

    .. versionadded:: 0.31.0

    .. warning::
        This function is experimental and subject to change.
    """
    if isinstance(actual, (str, Path)):
        actual = _load_xarray(actual)
    if isinstance(expected, (str, Path)):
        expected = _load_xarray(expected)

    if isinstance(actual, xarray.Dataset) and isinstance(expected, xarray.Dataset):
        assert_xarray_dataset_allclose(actual, expected, rtol=rtol, atol=atol)
    elif isinstance(actual, xarray.DataArray) and isinstance(expected, xarray.DataArray):
        assert_xarray_dataarray_allclose(actual, expected, rtol=rtol, atol=atol)
    else:
        raise ValueError(f"Unsupported types: {type(actual)} and {type(expected)}")


def _as_job_results_download(
    job_results: Union[BatchJob, JobResults, str, Path], tmp_path: Optional[Path] = None
) -> Path:
    """
    Produce a directory with downloaded job results assets and metadata.

    :param job_results: a batch job, job results metadata object or a path
    :param tmp_path: root temp path to download results if needed
    :return:
    """
    # TODO: support download/copy from other sources (e.g. S3, ...)
    if isinstance(job_results, BatchJob):
        job_results = job_results.get_results()
    if isinstance(job_results, JobResults):
        download_dir = tempfile.mkdtemp(dir=tmp_path, prefix=job_results.get_job_id() + "-")
        _log.info(f"Downloading results from job {job_results.get_job_id()} to {download_dir}")
        job_results.download_files(target=download_dir)
        job_results = download_dir
    if isinstance(job_results, (str, Path)):
        return Path(job_results)
    else:
        raise ValueError(f"Unsupported type: {type(job_results)}")


def _compare_job_results(
    actual: Union[BatchJob, JobResults, str, Path],
    expected: Union[BatchJob, JobResults, str, Path],
    *,
    rtol: float = _DEFAULT_RTOL,
    atol: float = _DEFAULT_ATOL,
    pixel_tolerance: float = _DEFAULT_PIXELTOL,
    tmp_path: Optional[Path] = None,
) -> List[str]:
    """
    Compare two job results sets (directories with downloaded assets and metadata,
    e.g. as produced by ``JobResults.download_files()``)

    :return: list of issues (empty if no issues)
    """
    actual_dir = _as_job_results_download(actual, tmp_path=tmp_path)
    expected_dir = _as_job_results_download(expected, tmp_path=tmp_path)
    _log.info(f"Comparing job results: {actual_dir!r} vs {expected_dir!r}")

    all_issues = []

    actual_filenames = set(p.name for p in actual_dir.glob("*") if p.is_file())
    expected_filenames = set(p.name for p in expected_dir.glob("*") if p.is_file())
    if actual_filenames != expected_filenames:
        all_issues.append(f"File set mismatch: {actual_filenames} != {expected_filenames}")

    for filename in expected_filenames.intersection(actual_filenames):
        actual_path = actual_dir / filename
        expected_path = expected_dir / filename
        if filename == DEFAULT_JOB_RESULTS_FILENAME:
            issues = _compare_job_result_metadata(actual=actual_path, expected=expected_path)
            if issues:
                all_issues.append(f"Issues for metadata file {filename!r}:")
                all_issues.extend(issues)
        elif expected_path.suffix.lower() in {".nc", ".netcdf"}:
            issues = _compare_xarray_datasets(
                actual=actual_path, expected=expected_path, rtol=rtol, atol=atol, pixel_tolerance=pixel_tolerance
            )
            if issues:
                all_issues.append(f"Issues for file {filename!r}:")
                all_issues.extend(issues)
        elif expected_path.suffix.lower() in {".tif", ".tiff", ".gtiff", ".geotiff"}:
            issues = _compare_xarray_dataarray(
                actual=actual_path, expected=expected_path, rtol=rtol, atol=atol, pixel_tolerance=pixel_tolerance
            )
            if issues:
                all_issues.append(f"Issues for file {filename!r}:")
                all_issues.extend(issues)
        else:
            _log.warning(f"Unhandled job result asset {filename!r}")

    return all_issues


def _compare_job_result_metadata(
    actual: Union[str, Path],
    expected: Union[str, Path],
) -> List[str]:
    issues = []
    actual_metadata = _load_json(actual)
    expected_metadata = _load_json(expected)

    # Check "derived_from" links
    actual_derived_from = set(k["href"] for k in actual_metadata.get("links", []) if k["rel"] == "derived_from")
    expected_derived_from = set(k["href"] for k in expected_metadata.get("links", []) if k["rel"] == "derived_from")

    if actual_derived_from != expected_derived_from:
        actual_only = actual_derived_from - expected_derived_from
        expected_only = expected_derived_from - actual_derived_from
        common = actual_derived_from.intersection(expected_derived_from)
        issues.append(
            f"Differing 'derived_from' links ({len(common)} common, {len(actual_only)} only in actual, {len(expected_only)} only in expected):\n"
            f"  only in actual: {repr_truncate(actual_only, width=1000)}\n"
            f"  only in expected: {repr_truncate(expected_only, width=1000)}."
        )

    # TODO: more metadata checks (e.g. spatial and temporal extents)?

    return issues


def assert_job_results_allclose(
    actual: Union[BatchJob, JobResults, str, Path],
    expected: Union[BatchJob, JobResults, str, Path],
    *,
    rtol: float = _DEFAULT_RTOL,
    atol: float = _DEFAULT_ATOL,
    pixel_tolerance: float = _DEFAULT_PIXELTOL,
    tmp_path: Optional[Path] = None,
):
    """
    Assert that two job results sets are equal (with tolerance).

    :param actual: actual job results, provided as :py:class:`~openeo.rest.job.BatchJob` object,
        :py:meth:`~openeo.rest.job.JobResults` object or path to directory with downloaded assets.
    :param expected: expected job results, provided as :py:class:`~openeo.rest.job.BatchJob` object,
        :py:meth:`~openeo.rest.job.JobResults` object or path to directory with downloaded assets.
    :param rtol: relative tolerance
    :param atol: absolute tolerance
    :param pixel_tolerance: maximum fraction of pixels (in percent)
        that is allowed to be significantly different (considering ``atol`` and ``rtol``)
    :param tmp_path: root temp path to download results if needed.
        It's recommended to pass pytest's `tmp_path` fixture here
    :raises AssertionError: if not equal within the given tolerance

    .. versionadded:: 0.31.0

    .. warning::
        This function is experimental and subject to change.
    """
    issues = _compare_job_results(
        actual, expected, rtol=rtol, atol=atol, pixel_tolerance=pixel_tolerance, tmp_path=tmp_path
    )
    if issues:
        raise AssertionError("\n".join(issues))
