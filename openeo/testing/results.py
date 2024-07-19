"""
Assert functions for comparing actual (batch job) results against expected reference data.
"""

import json
import logging
import tempfile
from pathlib import Path
from typing import List, Optional, Union

import xarray
import xarray.testing

from openeo.rest.job import DEFAULT_JOB_RESULTS_FILENAME, BatchJob, JobResults
from openeo.util import repr_truncate

_log = logging.getLogger(__name__)


_DEFAULT_RTOL = 1e-6
_DEFAULT_ATOL = 1e-6


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
    # provides detailed information about shape/dimension mismatches
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

    try:
        xarray.testing.assert_allclose(a=actual, b=expected, rtol=rtol, atol=atol)
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
) -> List[str]:
    """
    Compare two xarray ``DataSet``s with tolerance and report mismatch issues (as strings)

    :return: list of issues (empty if no issues)
    """
    # TODO: make this a public function?
    actual = _as_xarray_dataset(actual)
    expected = _as_xarray_dataset(expected)

    all_issues = []
    # TODO: just leverage DataSet support in xarray.testing.assert_allclose for all this?
    actual_vars = set(actual.data_vars)
    expected_vars = set(expected.data_vars)
    _log.debug(f"_compare_xarray_datasets: actual_vars={actual_vars!r} expected_vars={expected_vars!r}")
    if actual_vars != expected_vars:
        all_issues.append(f"Xarray DataSet variables mismatch: {actual_vars} != {expected_vars}")
    for var in expected_vars.intersection(actual_vars):
        _log.debug(f"_compare_xarray_datasets: comparing variable {var!r}")
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
    :param tmp_path: root temp path to download results if needed.
        It's recommended to pass pytest's `tmp_path` fixture here
    :raises AssertionError: if not equal within the given tolerance

    .. versionadded:: 0.31.0

    .. warning::
        This function is experimental and subject to change.
    """
    issues = _compare_job_results(actual, expected, rtol=rtol, atol=atol, tmp_path=tmp_path)
    if issues:
        raise AssertionError("\n".join(issues))
