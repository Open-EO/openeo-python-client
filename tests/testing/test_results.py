import contextlib
import json
import re
from pathlib import Path
from typing import List, Optional, Union

import dirty_equals
import numpy
import pytest
import xarray

from openeo.rest.job import DEFAULT_JOB_RESULTS_FILENAME
from openeo.testing.results import (
    _compare_xarray_dataarray,
    assert_job_results_allclose,
    assert_xarray_dataarray_allclose,
    assert_xarray_dataset_allclose,
)
from openeo.utils.version import ComparableVersion


class TestCompareXarray:
    def test_simple_defaults(self):
        expected = xarray.DataArray([1, 2, 3])
        actual = xarray.DataArray([1, 2, 3])
        issues = _compare_xarray_dataarray(actual, expected)
        assert issues == []


    @pytest.mark.parametrize(
        ["actual", "expected_issues"],
        [
            (
                xarray.DataArray([1, 2, 3, 4]),
                [
                    "Coordinates mismatch for dimension 'dim_0': [0 1 2 3] != [0 1 2]",
                    "Shape mismatch: (4,) != (3,)",
                ],
            ),
            (
                xarray.DataArray([[1, 2, 3], [4, 5, 6]]),
                [
                    "Dimension mismatch: ('dim_0', 'dim_1') != ('dim_0',)",
                    "Coordinates mismatch for dimension 'dim_0': [0 1] != [0 1 2]",
                    "Shape mismatch: (2, 3) != (3,)",
                ],
            ),
            (
                xarray.DataArray([[1], [2], [3]]),
                [
                    "Dimension mismatch: ('dim_0', 'dim_1') != ('dim_0',)",
                    "Shape mismatch: (3, 1) != (3,)",
                ],
            ),
        ],
    )
    def test_simple_shape_mismatch(self, actual, expected_issues):
        expected = xarray.DataArray([1, 2, 3])
        assert _compare_xarray_dataarray(actual=actual, expected=expected) == expected_issues

    @pytest.mark.parametrize(
        ["actual", "expected_issues"],
        [
            (
                xarray.DataArray([[1, 2, 3], [4, 5, 6]], dims=["x", "y"]),
                [],
            ),
            (
                xarray.DataArray([[1, 2, 3], [4, 5, 6]], dims=["y", "x"]),
                [
                    "Dimension mismatch: ('y', 'x') != ('x', 'y')",
                    "Coordinates mismatch for dimension 'x': [0 1 2] != [0 1]",
                    "Coordinates mismatch for dimension 'y': [0 1] != [0 1 2]",
                ],
            ),
            (
                xarray.DataArray([[1, 2, 3], [4, 5, 6]], dims=["x", "z"]),
                [
                    "Dimension mismatch: ('x', 'z') != ('x', 'y')",
                ],
            ),
        ],
    )
    def test_simple_dims_mismatch(self, actual, expected_issues):
        expected = xarray.DataArray([[1, 2, 3], [4, 5, 6]], dims=["x", "y"])
        assert _compare_xarray_dataarray(actual=actual, expected=expected) == expected_issues

    @pytest.mark.parametrize(
        ["actual", "expected_issues"],
        [
            (
                xarray.DataArray([[1, 2, 3], [4, 5, 6]], coords=[("x", [11, 22]), ("y", [33, 44, 55])]),
                [],
            ),
            (
                xarray.DataArray([[1, 2, 3], [4, 5, 6]], coords=[("x", [111, 222]), ("y", [33, 44, 55])]),
                [
                    "Coordinates mismatch for dimension 'x': [111 222] != [11 22]",
                ],
            ),
        ],
    )
    def test_simple_coords_mismatch(self, actual, expected_issues):
        expected = xarray.DataArray([[1, 2, 3], [4, 5, 6]], coords=[("x", [11, 22]), ("y", [33, 44, 55])])
        assert _compare_xarray_dataarray(actual=actual, expected=expected) == expected_issues

    @pytest.mark.parametrize(
        ["actual", "rtol", "expected_issues"],
        [
            (xarray.DataArray([1, 2, 3.0000001]), 1e-6, []),
            (
                xarray.DataArray([1, 2, 3.00001]),
                1e-6,
                [dirty_equals.IsStr(regex=r"Left and right DataArray objects are not close.*", regex_flags=re.DOTALL)],
            ),
            (xarray.DataArray([1, 2, 3.001]), 0.1, []),
        ],
    )
    def test_simple_rtol(self, actual, rtol, expected_issues):
        expected = xarray.DataArray([1, 2, 3])
        assert _compare_xarray_dataarray(actual=actual, expected=expected, rtol=rtol) == expected_issues

    @pytest.mark.parametrize(
        ["actual", "atol", "expected_issues"],
        [
            (xarray.DataArray([1, 2, 3.001]), 0.01, []),
            (
                xarray.DataArray([1, 2, 3.1]),
                0.1,
                [dirty_equals.IsStr(regex=r"Left and right DataArray objects are not close.*", regex_flags=re.DOTALL)],
            ),
            (xarray.DataArray([1, 2, 3.1]), 0.2, []),
        ],
    )
    def test_simple_atol(self, actual, atol, expected_issues):
        expected = xarray.DataArray([1, 2, 3])
        assert _compare_xarray_dataarray(actual=actual, expected=expected, rtol=0, atol=atol) == expected_issues


    def test_nan_handling(self):
        expected = xarray.DataArray([1, 2, numpy.nan, 4, float("nan")])
        actual = xarray.DataArray([1, 2, numpy.nan, 4.001, float("nan")])

        assert _compare_xarray_dataarray(actual, expected, rtol=0, atol=0.01) == []
        assert _compare_xarray_dataarray(actual, expected, rtol=0, atol=0.0001) == [
            dirty_equals.IsStr(regex=r"Left and right DataArray objects are not close.*", regex_flags=re.DOTALL),
        ]


@contextlib.contextmanager
def raises_assertion_error_or_not(message: Union[None, str, re.Pattern]):
    """
    Helper to set up a context that expects:
    - an AssertionError that matches a given message
    - or no exception at all if no message is given
    """
    if message:
        if isinstance(message, str):
            message = re.compile(message, flags=re.DOTALL)
        with pytest.raises(AssertionError, match=message):
            yield
    else:
        yield


class TestAssertXarray:
    def test_assert_xarray_dataarray_allclose_minimal(self):
        expected = xarray.DataArray([1, 2, 3])
        actual = xarray.DataArray([1, 2, 3])
        assert_xarray_dataarray_allclose(actual=actual, expected=expected)

    def test_assert_xarray_dataarray_allclose_shape_mismatch(self):
        expected = xarray.DataArray([1, 2, 3])
        actual = xarray.DataArray([1, 2, 3, 4])
        with raises_assertion_error_or_not(
            r"Coordinates mismatch for dimension 'dim_0': \[0 1 2 3\] != \[0 1 2\].*Shape mismatch: \(4,\) != \(3,\)"
        ):
            assert_xarray_dataarray_allclose(actual=actual, expected=expected)

    def test_assert_xarray_dataarray_allclose_coords_mismatch(self):
        expected = xarray.DataArray([[1, 2, 3], [4, 5, 6]], coords=[("space", [11, 22]), ("time", [33, 44, 55])])
        actual = xarray.DataArray([[1, 2, 3], [4, 5, 6]], coords=[("space", [11, 666]), ("time", [33, 44, 55])])
        with raises_assertion_error_or_not(r"Coordinates mismatch for dimension 'space': \[ 11 666\] != \[11 22\]"):
            assert_xarray_dataarray_allclose(actual=actual, expected=expected)

    @pytest.mark.parametrize(
        ["kwargs", "assertion_error"],
        [
            ({}, r"Left and right DataArray objects are not close.*"),
            ({"rtol": 0.01}, r"Left and right DataArray objects are not close.*"),
            ({"rtol": 0.1}, None),
            ({"atol": 0.2}, r"Left and right DataArray objects are not close.*"),
            ({"atol": 0.3}, None),
            ({"rtol": 0.01, "atol": 0.2}, None),
        ],
    )
    def test_assert_xarray_dataarray_allclose_tolerance(self, kwargs, assertion_error):
        expected = xarray.DataArray([1, 2, 3])
        actual = xarray.DataArray([1, 2, 3.21])
        with raises_assertion_error_or_not(message=assertion_error):
            assert_xarray_dataarray_allclose(actual=actual, expected=expected, **kwargs)

    def test_assert_xarray_dataset_allclose_minimal(self):
        expected = xarray.Dataset({"a": xarray.DataArray([1, 2, 3])})
        actual = xarray.Dataset({"a": xarray.DataArray([1, 2, 3])})
        assert_xarray_dataset_allclose(actual=actual, expected=expected)

    def test_assert_xarray_dataset_allclose_basic(self):
        expected = xarray.Dataset(
            {
                "a": (["time"], [1, 2, 3]),
                "b": (["time"], [4, 5, 6]),
            },
            coords={"time": [11, 22, 33]},
        )
        actual = xarray.Dataset(
            {
                "a": (["time"], [1, 2, 3]),
                "b": (["time"], [4, 5, 6]),
            },
            coords={"time": [11, 22, 33]},
        )
        assert_xarray_dataset_allclose(actual=actual, expected=expected)

    def test_assert_xarray_dataset_allclose_shape_mismatch(self):
        expected = xarray.Dataset(
            {
                "a": (["time"], [1, 2, 3]),
                "b": (["time"], [4, 5, 6]),
            },
            coords={"time": [11, 22, 33]},
        )
        actual = xarray.Dataset(
            {
                "a": (["time", "space"], [[1], [2], [3]]),
                "b": (["time"], [4, 5, 6]),
            },
            coords={"time": [11, 22, 33], "space": [777]},
        )
        with raises_assertion_error_or_not(
            r"Issues for variable 'a'.*Dimension mismatch: \('time', 'space'\) != \('time',\).*Shape mismatch: \(3, 1\) != \(3,\)"
        ):
            assert_xarray_dataset_allclose(actual=actual, expected=expected)

    def test_assert_xarray_dataset_allclose_coords_mismatch(self):
        expected = xarray.Dataset(
            {
                "a": (["time"], [1, 2, 3]),
                "b": (["time"], [4, 5, 6]),
            },
            coords={"time": [11, 22, 33]},
        )
        actual = xarray.Dataset(
            {
                "a": (["time"], [1, 2, 3]),
                "b": (["time"], [4, 5, 6]),
            },
            coords={"time": [11, 22, 666]},
        )
        with raises_assertion_error_or_not(
            r"Issues for variable 'a':.*Coordinates mismatch for dimension 'time': \[ 11  22 666\] != \[11 22 33\]"
        ):
            assert_xarray_dataset_allclose(actual=actual, expected=expected)

    @pytest.mark.parametrize(
        ["kwargs", "assertion_error"],
        [
            ({}, r"Issues for variable 'b':.*Left and right DataArray objects are not close.*"),
            ({"rtol": 0.01}, r"Issues for variable 'b':.*Left and right DataArray objects are not close.*"),
            ({"rtol": 0.1}, None),
            ({"atol": 0.2}, r"Issues for variable 'b':.*Left and right DataArray objects are not close.*"),
            ({"atol": 0.3}, None),
            ({"rtol": 0.01, "atol": 0.2}, None),
        ],
    )
    def test_assert_xarray_dataset_allclose_tolerance(self, kwargs, assertion_error):
        expected = xarray.Dataset(
            {
                "a": (["time"], [1, 2, 3]),
                "b": (["time"], [4, 5, 6]),
            },
            coords={"time": [11, 22, 33]},
        )
        actual = xarray.Dataset(
            {
                "a": (["time"], [1, 2, 3]),
                "b": (["time"], [4, 5, 6.23]),
            },
            coords={"time": [11, 22, 33]},
        )
        with raises_assertion_error_or_not(message=assertion_error):
            assert_xarray_dataset_allclose(actual=actual, expected=expected, **kwargs)

    @pytest.mark.skipif(
        ComparableVersion(xarray.__version__) < "2024.07.0" and ComparableVersion(numpy.__version__) >= "2.0.0",
        reason="This test doesn't work due to numpy 2 compatibility issue in xarray below 2024.7.0",
    )
    def test_assert_xarray_dataset_allclose_empty_coords_handling(self):
        expected = xarray.Dataset(
            {
                "b02": xarray.DataArray([1, 2, 3]),
                "crs": xarray.DataArray(b"", attrs={"spatial_ref": "meh"}),
            }
        )
        actual = xarray.Dataset(
            {
                "b02": xarray.DataArray([1, 2, 3]),
                "crs": xarray.DataArray(b"", attrs={"spatial_ref": "meh"}),
            }
        )
        assert_xarray_dataset_allclose(actual=actual, expected=expected)


class TestAssertJobResults:
    @pytest.fixture
    def actual_dir(self, tmp_path) -> Path:
        actual_dir = tmp_path / "actual"
        actual_dir.mkdir()
        return actual_dir

    @pytest.fixture
    def expected_dir(self, tmp_path) -> Path:
        expected_dir = tmp_path / "expected"
        expected_dir.mkdir()
        return expected_dir

    def test_allclose_minimal(self, tmp_path, actual_dir, expected_dir):
        (expected_dir / "readme.md").write_text("Hello world")
        (actual_dir / "readme.md").write_text("Wello Horld")
        assert_job_results_allclose(actual=actual_dir, expected=expected_dir, tmp_path=tmp_path)

    def test_allclose_minimal_success(self, tmp_path, actual_dir, expected_dir):
        ds = xarray.Dataset({"a": (["time"], [1, 2, 3])}, coords={"time": [11, 22, 33]})
        ds.to_netcdf(expected_dir / "data.nc")
        ds.to_netcdf(actual_dir / "data.nc")
        assert_job_results_allclose(actual=actual_dir, expected=expected_dir, tmp_path=tmp_path)

    def test_allclose_xy_success(self, tmp_path, actual_dir, expected_dir):
        expected_ds = xarray.Dataset(
            {
                "b1": xarray.Variable(dims=["t", "x", "y"], data=2 * numpy.ones((3, 4, 5))),
                "b2": xarray.Variable(dims=["t", "x", "y"], data=3 * numpy.ones((3, 4, 5))),
            },
            coords={
                "t": range(0, 3),
                "x": range(4, 8),
                "y": range(5, 10),
            },
        )
        expected_ds.to_netcdf(expected_dir / "data.nc")
        actual_ds = xarray.Dataset(
            {
                "b1": xarray.Variable(dims=["t", "x", "y"], data=1 * numpy.ones((3, 4, 5))),
                "b2": xarray.Variable(dims=["t", "x", "y"], data=3 * numpy.ones((3, 4, 5))),
            },
            coords={
                "t": range(0, 3),
                "x": range(4, 8),
                "y": range(5, 10),
            },
        )
        actual_ds.to_netcdf(actual_dir / "data.nc")
        assert_job_results_allclose(actual=actual_dir, expected=expected_dir, tmp_path=tmp_path, rtol=1)

    def test_allclose_minimal_xy_different(self, tmp_path, actual_dir, expected_dir):
        expected_ds = xarray.Dataset(
            {
                "b1": xarray.Variable(dims=["t", "x", "y"], data=2 * numpy.ones((3, 4, 5))),
                "b2": xarray.Variable(dims=["t", "x", "y"], data=3 * numpy.ones((3, 4, 5))),
            },
            coords={
                "t": range(0, 3),
                "x": range(4, 8),
                "y": range(5, 10),
            },
        )
        expected_ds.to_netcdf(expected_dir / "data.nc")
        actual_ds = xarray.Dataset(
            {
                "b1": xarray.Variable(dims=["t", "x", "y"], data=1 * numpy.ones((3, 4, 5))),
                "b2": xarray.Variable(dims=["t", "x", "y"], data=3 * numpy.ones((3, 4, 5))),
            },
            coords={
                "t": range(0, 3),
                "x": range(4, 8),
                "y": range(5, 10),
            },
        )
        actual_ds.to_netcdf(actual_dir / "data.nc")
        with raises_assertion_error_or_not(
            r"Issues for file 'data.nc'.*"
            r"Issues for variable 'b1'.*"
            r"t 0: value difference min:1.0, max: 1.0, mean: 1.0, var: 0.0.*"
            r"t 0: differing pixels: 20/20 \(100.0%\), spread over 100.0% of the area.*"
            r"t 1: value difference min:1.0, max: 1.0, mean: 1.0, var: 0.0.*"
            r"t 1: differing pixels: 20/20 \(100.0%\), spread over 100.0% of the area.*"
            r"t 2: value difference min:1.0, max: 1.0, mean: 1.0, var: 0.0.*"
            r"t 2: differing pixels: 20/20 \(100.0%\), spread over 100.0% of the area"
        ):
            assert_job_results_allclose(actual=actual_dir, expected=expected_dir, tmp_path=tmp_path)

    def test_allclose_minimal_xy_different_small_area(self, tmp_path, actual_dir, expected_dir):
        expected_ds = xarray.Dataset(
            {
                "b1": xarray.Variable(dims=["t", "x", "y"], data=2 * numpy.ones((3, 4, 5))),
                "b2": xarray.Variable(dims=["t", "x", "y"], data=3 * numpy.ones((3, 4, 5))),
            },
            coords={
                "t": range(0, 3),
                "x": range(4, 8),
                "y": range(5, 10),
            },
        )
        expected_ds.to_netcdf(expected_dir / "data.nc")
        b2_modified_data = 3 * numpy.ones((3, 4, 5))
        b2_modified_data[2][2][2] *= 15
        b2_modified_data[2][2][3] *= 14
        b2_modified_data[2][3][2] *= 13
        b2_modified_data[2][3][3] *= 12
        actual_ds = xarray.Dataset(
            {
                "b1": xarray.Variable(dims=["t", "x", "y"], data=2 * numpy.ones((3, 4, 5))),
                "b2": xarray.Variable(dims=["t", "x", "y"], data=b2_modified_data),
            },
            coords={
                "t": range(0, 3),
                "x": range(4, 8),
                "y": range(5, 10),
            },
        )
        actual_ds.to_netcdf(actual_dir / "data.nc")
        with raises_assertion_error_or_not(
            r"Issues for file 'data.nc'.*"
            r"Issues for variable 'b2'.*"
            r"t 2: value difference min:33.0, max: 42.0, mean: 37.5, var: 11.2.*"
            r"t 2: differing pixels: 4/20 \(20.0%\), spread over 8.3% of the area"
        ):
            assert_job_results_allclose(actual=actual_dir, expected=expected_dir, tmp_path=tmp_path)

    def test_allclose_basic_fail(self, tmp_path, actual_dir, expected_dir):
        expected_ds = xarray.Dataset({"a": (["time"], [1, 2, 3])}, coords={"time": [11, 22, 33]})
        expected_ds.to_netcdf(expected_dir / "data.nc")
        actual_ds = xarray.Dataset({"a": (["time"], [1, 2, 3.21])}, coords={"time": [11, 22, 33]})
        actual_ds.to_netcdf(actual_dir / "data.nc")
        with raises_assertion_error_or_not(
            r"Issues for file 'data.nc'.*Issues for variable 'a'.*Left and right DataArray objects are not close.*"
        ):
            assert_job_results_allclose(actual=actual_dir, expected=expected_dir, tmp_path=tmp_path)

    def _create_metadata_json_file(self, path: Path, *, links: Optional[List[dict]] = None):
        metadata = {}
        if links:
            metadata["links"] = links
        path.write_text(json.dumps(metadata))

    def test_assert_job_results_allclose_derived_from_match(self, tmp_path, actual_dir, expected_dir):
        self._create_metadata_json_file(
            path=actual_dir / DEFAULT_JOB_RESULTS_FILENAME,
            links=[
                {"rel": "derived_from", "href": "/path/to/S2B_blabla_1.SAFE"},
                {"rel": "derived_from", "href": "/path/to/S2B_blabla_2.SAFE"},
            ],
        )
        self._create_metadata_json_file(
            path=expected_dir / DEFAULT_JOB_RESULTS_FILENAME,
            links=[
                {"rel": "derived_from", "href": "/path/to/S2B_blabla_1.SAFE"},
                {"rel": "derived_from", "href": "/path/to/S2B_blabla_2.SAFE"},
            ],
        )
        assert_job_results_allclose(actual=actual_dir, expected=expected_dir, tmp_path=tmp_path)

    def test_assert_job_results_allclose_derived_from_mismatch(self, tmp_path, actual_dir, expected_dir):
        self._create_metadata_json_file(
            path=actual_dir / DEFAULT_JOB_RESULTS_FILENAME,
            links=[
                {"rel": "derived_from", "href": "/path/to/S2B_blabla_2.SAFE"},
                {"rel": "derived_from", "href": "/path/to/S2B_blabla_666.SAFE"},
            ],
        )
        self._create_metadata_json_file(
            path=expected_dir / DEFAULT_JOB_RESULTS_FILENAME,
            links=[
                {"rel": "derived_from", "href": "/path/to/S2B_blabla_1.SAFE"},
                {"rel": "derived_from", "href": "/path/to/S2B_blabla_2.SAFE"},
                {"rel": "derived_from", "href": "/path/to/S2B_blabla_3.SAFE"},
            ],
        )
        with raises_assertion_error_or_not(
            message="Differing 'derived_from' links.*1 common, 1 only in actual, 2 only in expected.*only in actual.*bla_666.*only in expected.*bla_3"
        ):
            assert_job_results_allclose(actual=actual_dir, expected=expected_dir, tmp_path=tmp_path)
