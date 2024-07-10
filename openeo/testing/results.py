"""
Tools for matching actual results against expected reference data.
"""

import logging
import tempfile
from pathlib import Path
from typing import List, Optional, Union

import numpy
import xarray

from openeo.rest.job import DEFAULT_JOB_RESULTS_FILENAME, BatchJob, JobResults

_log = logging.getLogger(__name__)


_DEFAULT_RTOL = 1e-6
_DEFAULT_ATOL = 1e-6


def _load_xarray_netcdf(path: Union[str, Path], **kwargs) -> xarray.Dataset:
    """
    Load a netCDF file as Xarray Dataset
    """
    return xarray.load_dataset(path, **kwargs)


def _load_rioxarray_geotiff(path: Union[str, Path], **kwargs) -> xarray.DataArray:
    """
    Load a GeoTIFF file as Xarray DataArray (using `rioxarray` extension).
    """
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


def _compare_xarray_dataarray(
    actual: Union[xarray.DataArray, str, Path],
    expected: Union[xarray.DataArray, str, Path],
    *,
    rtol: float = _DEFAULT_RTOL,
    atol: float = _DEFAULT_ATOL,
) -> List[str]:
    """
    Compare two xarray DataArrays with tolerance and report mismatch issues (as strings)

    Checks that are done (with tolerance):
    - (optional) Check fraction of mismatching pixels (difference exceeding some tolerance).
      If fraction is below a given threshold, ignore these mismatches in subsequent comparisons.
      If fraction is above the threshold, report this issue.
    - Compare actual and expected data with `numpy.testing.assert_allclose` and specified tolerances.

    :return: list of issues (empty if no issues)
    """
    # TODO: make this a public function?
    # TODO: option for nodata fill value?
    # TODO: option to include data type check?
    # TODO: option to cast to some data type (or even rescale) before comparison?
    actual = _as_xarray_dataarray(actual)
    expected = _as_xarray_dataarray(expected)
    issues = []

    # TODO: isn't there functionality in Xarray itself to compare these aspects?
    if actual.dims != expected.dims:
        issues.append(f"Dimension mismatch: {actual.dims} != {expected.dims}")
    for dim in set(expected.dims).intersection(actual.dims):
        acs = actual.coords[dim].values
        ecs = expected.coords[dim].values
        if not (acs.shape == ecs.shape and (acs == ecs).all()):
            issues.append(f"Coordinates mismatch for dimension {dim!r}: {acs} != {ecs}")
    if actual.shape != expected.shape:
        issues.append(f"Shape mismatch: {actual.shape} != {expected.shape}")
        # Abort: no point in comparing data if shapes do not match
        return issues

    try:
        numpy.testing.assert_allclose(actual=actual, desired=expected, rtol=rtol, atol=atol, equal_nan=True)
    except AssertionError as e:
        # TODO: message of `assert_allclose` is typically multiline, split it again or make it one line?
        issues.append(str(e).strip())

    return issues


def assert_xarray_dataarray_allclose(
    actual: Union[xarray.DataArray, str, Path],
    expected: Union[xarray.DataArray, str, Path],
    *,
    rtol: float = _DEFAULT_RTOL,
    atol: float = _DEFAULT_ATOL,
):
    """
    Assert that two Xarray ``DataArrays`` are almost equal (with tolerance).

    :param actual: actual value
    :param expected: expected or reference value
    :param rtol: relative tolerance
    :param atol: absolute tolerance
    :raises AssertionError: if not equal within the given tolerance

    .. warning::
        This function is experimental and its API may change in the future.
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
) -> List[str]:
    """
    Compare two xarray DataSets with tolerance and report mismatch issues (as strings)

    :return: list of issues (empty if no issues)
    """
    # TODO: make this a public function?
    actual = _as_xarray_dataset(actual)
    expected = _as_xarray_dataset(expected)

    all_issues = []
    actual_vars = set(actual.data_vars)
    expected_vars = set(expected.data_vars)
    if actual_vars != expected_vars:
        all_issues.append(f"Xarray DataSet variables mismatch: {actual_vars} != {expected_vars}")
    for var in expected_vars.intersection(actual_vars):
        issues = _compare_xarray_dataarray(actual[var], expected[var], rtol=rtol, atol=atol)
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
    Assert that two Xarray ``DataSets`` are almost equal (with tolerance).

    :param actual: actual value
    :param expected: expected or reference value
    :param rtol: relative tolerance
    :param atol: absolute tolerance
    :raises AssertionError: if not equal within the given tolerance

    .. warning::
        This function is experimental and its API may change in the future.
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
    Assert that two Xarray ``DataSet``s or ``DataArray``s are equal (with tolerance).

    :param actual: actual data array or dataset
    :param expected: actual data array or dataset
    :param rtol: relative tolerance
    :param atol: absolute tolerance
    :raises AssertionError: if not equal within the given tolerance

    .. warning::
        This function is experimental and its API may change in the future.
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
            # TODO: check metadata (e.g. "derived_from" links)
            ...
        elif expected_path.suffix.lower() in {".nc", ".netcdf"}:
            issues = _compare_xarray_datasets(actual=actual_path, expected=expected_path, rtol=rtol, atol=atol)
            if issues:
                all_issues.append(f"Issues for file {filename!r}:")
                all_issues.extend(issues)
        elif expected_path.suffix.lower() in {".tif", ".tiff", ".gtiff", ".geotiff"}:
            issues = _compare_xarray_dataarray(actual=actual_path, expected=expected_path, rtol=rtol, atol=atol)
            if issues:
                all_issues.append(f"Issues for file {filename!r}:")
                all_issues.extend(issues)
        else:
            _log.warning(f"Unhandled job result asset {filename!r}")

    return all_issues


def assert_job_results_allclose(
    actual: Union[BatchJob, JobResults, str, Path],
    expected: Union[BatchJob, JobResults, str, Path],
    *,
    rtol: float = _DEFAULT_RTOL,
    atol: float = _DEFAULT_ATOL,
    tmp_path: Optional[Path] = None,
):
    """
    Assert that two job results sets are almost equal (with tolerance).

    :param actual: actual job results (batch job, job results metadata object or a path)
    :param expected: expected job results (batch job, job results metadata object or a path)
    :param rtol: relative tolerance
    :param atol: absolute tolerance
    :param tmp_path: root temp path to download results if needed.
        It's recommended to pass pytest's `tmp_path` fixture here
    :raises AssertionError: if not equal within the given tolerance

    .. warning::
        This function is experimental and its API may change in the future.
    """
    issues = _compare_job_results(actual, expected, rtol=rtol, atol=atol, tmp_path=tmp_path)
    if issues:
        raise AssertionError("\n".join(issues))
