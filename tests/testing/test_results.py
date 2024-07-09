import re

import dirty_equals
import numpy
import pytest
import xarray

from openeo.testing.results import compare_xarray


class TestCompareXarray:
    def test_simple_defaults(self):
        desired = xarray.DataArray([1, 2, 3])
        actual = xarray.DataArray([1, 2, 3])
        issues = compare_xarray(actual, desired)
        assert issues == []

    @pytest.mark.parametrize(
        ["actual", "expected"],
        [
            (xarray.DataArray([1, 2, 3, 4]), ["Shape mismatch: (4,) != (3,)"]),
            (xarray.DataArray([[1, 2, 3], [4, 5, 6]]), ["Shape mismatch: (2, 3) != (3,)"]),
            (xarray.DataArray([[1], [2], [3]]), ["Shape mismatch: (3, 1) != (3,)"]),
        ],
    )
    def test_simple_shape_mismatch(self, actual, expected):
        desired = xarray.DataArray([1, 2, 3])
        assert compare_xarray(actual=actual, desired=desired) == expected

    @pytest.mark.parametrize(
        ["actual", "rtol", "expected"],
        [
            (xarray.DataArray([1, 2, 3.0000001]), 1e-6, []),
            (
                xarray.DataArray([1, 2, 3.00001]),
                1e-6,
                [
                    dirty_equals.IsStr(
                        regex=r"Not equal to tolerance rtol=1e-06, atol=0\s.*Mismatched elements: 1 / 3\s.*Max absolute difference.*?: 1\.e-05\s.*",
                        regex_flags=re.DOTALL,
                    )
                ],
            ),
            (xarray.DataArray([1, 2, 3.001]), 0.1, []),
        ],
    )
    def test_simple_rtol(self, actual, rtol, expected):
        desired = xarray.DataArray([1, 2, 3])
        assert compare_xarray(actual=actual, desired=desired, rtol=rtol) == expected

    @pytest.mark.parametrize(
        ["actual", "atol", "expected"],
        [
            (xarray.DataArray([1, 2, 3.001]), 0.01, []),
            (
                xarray.DataArray([1, 2, 3.1]),
                0.1,
                [
                    dirty_equals.IsStr(
                        regex=r"Not equal to tolerance rtol=0, atol=0\.1\s.*Mismatched elements: 1 / 3\s.*Max absolute difference.*?: 0\.1\s.*",
                        regex_flags=re.DOTALL,
                    )
                ],
            ),
            (xarray.DataArray([1, 2, 3.1]), 0.2, []),
        ],
    )
    def test_simple_atol(self, actual, atol, expected):
        desired = xarray.DataArray([1, 2, 3])
        assert compare_xarray(actual=actual, desired=desired, rtol=0, atol=atol) == expected

    @pytest.mark.parametrize(
        ["allowed_mismatch_fraction", "expected"],
        [
            (0.1, []),
            (
                0.01,
                [
                    "Fraction of mismatched elements is too large: 6.00% > 1.00%",
                    dirty_equals.IsStr(
                        regex=r"Not equal to tolerance rtol=1e-07, atol=0\s.*Mismatched elements: 6 / 100\s.*Max absolute difference.*?: 1\.\s.*",
                        regex_flags=re.DOTALL,
                    ),
                ],
            ),
        ],
    )
    def test_allowed_mismatch_fraction(self, allowed_mismatch_fraction, expected):
        desired = xarray.DataArray(numpy.ones((10, 10)))
        actual = xarray.DataArray(numpy.ones((10, 10)))
        actual[1:3, 1:4] = 2
        assert (
            compare_xarray(
                actual=actual,
                desired=desired,
                allowed_mismatch_fraction=allowed_mismatch_fraction,
            )
            == expected
        )

    @pytest.mark.parametrize(
        ["rtol", "allowed_mismatch_fraction", "allowed_mismatch_rtol", "expected"],
        [
            pytest.param(2, 0.01, 2, [], id="generous-rtols"),
            pytest.param(0.11, 0.1, 0.2, [], id="just-below-radar"),
            pytest.param(
                0.01,
                0.1,
                0.5,
                [
                    dirty_equals.IsStr(
                        regex=r"Not equal to tolerance rtol=0\.01, atol=0\s.*Mismatched elements: 16 / 100\s.*Max absolute difference.*?: 0\.1\s.*",
                        regex_flags=re.DOTALL,
                    )
                ],
                id="tight-rtol",
            ),
            pytest.param(
                0.01,
                0.01,
                0.5,
                [
                    "Fraction of mismatched elements is too large: 6.00% > 1.00%",
                    dirty_equals.IsStr(
                        regex=r"Not equal to tolerance rtol=0\.01, atol=0\s.*Mismatched elements: 22 / 100\s.*Max absolute difference.*?: 1\.\s.*",
                        regex_flags=re.DOTALL,
                    ),
                ],
                id="tight-rtol-and-fraction",
            ),
            pytest.param(
                2,
                0.1,
                0.01,
                ["Fraction of mismatched elements is too large: 22.00% > 10.00%"],
                id="tight-allowed-rtol",
            ),
            pytest.param(0.001, 0.3, 0.09, [], id="generous-allowed-rtol"),
            pytest.param(0.2, 0.1, None, [], id="default-allowed-rtol-just-below-radar"),
            pytest.param(
                0.2,
                0.01,
                None,
                [
                    "Fraction of mismatched elements is too large: 6.00% > 1.00%",
                    dirty_equals.IsStr(
                        regex=r"Not equal to tolerance rtol=0\.2, atol=0\s.*Mismatched elements: 6 / 100\s.*Max absolute difference.*?: 1\.\s.*",
                        regex_flags=re.DOTALL,
                    ),
                ],
                id="default-allowed-rtol-with-small-allowed-fraction",
            ),
            pytest.param(
                0.08,
                0.1,
                None,
                [
                    "Fraction of mismatched elements is too large: 22.00% > 10.00%",
                    dirty_equals.IsStr(
                        regex=r"Not equal to tolerance rtol=0\.08, atol=0\s.*Mismatched elements: 22 / 100\s.*Max absolute difference.*?: 1\.\s.*",
                        regex_flags=re.DOTALL,
                    ),
                ],
                id="default-allowed-rtol-with-tight-rtol",
            ),
        ],
    )
    def test_allowed_mismatch_fraction_with_rtol(
        self, rtol, allowed_mismatch_fraction, allowed_mismatch_rtol, expected
    ):
        desired = xarray.DataArray(numpy.ones((10, 10)))
        actual = xarray.DataArray(numpy.ones((10, 10)))
        actual[1:3, 1:4] = 2
        actual[5:9, 5:9] = 1.1
        assert (
            compare_xarray(
                actual=actual,
                desired=desired,
                rtol=rtol,
                atol=0,
                allowed_mismatch_fraction=allowed_mismatch_fraction,
                allowed_mismatch_rtol=allowed_mismatch_rtol,
                allowed_mismatch_atol=0,
            )
            == expected
        )

    @pytest.mark.parametrize(
        ["atol", "allowed_mismatch_fraction", "allowed_mismatch_atol", "expected"],
        [
            pytest.param(2, 0.01, 2, [], id="generous-atols"),
            pytest.param(0.11, 0.1, 0.25, [], id="just-below-radar"),
            pytest.param(
                0.012,
                0.1,
                0.5,
                [
                    dirty_equals.IsStr(
                        regex=r"Not equal to tolerance rtol=0, atol=0\.012\s.*Mismatched elements: 16 / 100\s.*Max absolute difference.*?: 0\.1\s.*",
                        regex_flags=re.DOTALL,
                    )
                ],
                id="tight-atol",
            ),
            pytest.param(
                0.012,
                0.01,
                0.5,
                [
                    "Fraction of mismatched elements is too large: 6.00% > 1.00%",
                    dirty_equals.IsStr(
                        regex=r"Not equal to tolerance rtol=0, atol=0.012\s.*Mismatched elements: 22 / 100\s.*Max absolute difference.*?: 1\.\s.*",
                        regex_flags=re.DOTALL,
                    ),
                ],
                id="tight-atol-and-fraction",
            ),
            pytest.param(
                2,
                0.1,
                0.01,
                ["Fraction of mismatched elements is too large: 22.00% > 10.00%"],
                id="tight-allowed-atol",
            ),
            pytest.param(0.001, 0.3, 0.09, [], id="generous-allowed-atol"),
            pytest.param(0.2, 0.1, None, [], id="default-allowed-atol-just-below-radar"),
            pytest.param(
                0.2,
                0.01,
                None,
                [
                    "Fraction of mismatched elements is too large: 6.00% > 1.00%",
                    dirty_equals.IsStr(
                        regex=r"Not equal to tolerance rtol=0, atol=0\.2\s.*Mismatched elements: 6 / 100\s.*Max absolute difference.*?: 1\.\s.*",
                        regex_flags=re.DOTALL,
                    ),
                ],
                id="default-allowed-atol-with-small-allowed-fraction",
            ),
            pytest.param(
                0.08,
                0.1,
                None,
                [
                    "Fraction of mismatched elements is too large: 22.00% > 10.00%",
                    dirty_equals.IsStr(
                        regex=r"Not equal to tolerance rtol=0, atol=0\.08\s.*Mismatched elements: 22 / 100\s.*Max absolute difference.*?: 1\.\s.*",
                        regex_flags=re.DOTALL,
                    ),
                ],
                id="default-allowed-rtol-with-tight-atol",
            ),
        ],
    )
    def test_allowed_mismatch_fraction_with_atol(
        self, atol, allowed_mismatch_fraction, allowed_mismatch_atol, expected
    ):
        desired = xarray.DataArray(numpy.ones((10, 10)))
        actual = xarray.DataArray(numpy.ones((10, 10)))
        actual[1:3, 1:4] = 2
        actual[5:9, 5:9] = 1.1
        assert (
            compare_xarray(
                actual=actual,
                desired=desired,
                rtol=0,
                atol=atol,
                allowed_mismatch_fraction=allowed_mismatch_fraction,
                allowed_mismatch_rtol=0,
                allowed_mismatch_atol=allowed_mismatch_atol,
            )
            == expected
        )

    def test_nan_handling(self):
        desired = xarray.DataArray([1, 2, numpy.nan, 4, float("nan")])
        actual = xarray.DataArray([1, 2, numpy.nan, 4.001, float("nan")])

        assert compare_xarray(actual, desired, rtol=0, atol=0.01) == []
        assert compare_xarray(actual, desired, rtol=0, atol=0.0001) == [
            dirty_equals.IsStr(
                regex=r"Not equal to tolerance rtol=0, atol=0\.0001\s.*Mismatched elements: 1 / 5\s.*Max absolute difference.*?: 0\.001\s.*",
                regex_flags=re.DOTALL,
            ),
        ]
        assert compare_xarray(actual, desired, rtol=0, atol=0.0001, allowed_mismatch_fraction=0.21) == []
