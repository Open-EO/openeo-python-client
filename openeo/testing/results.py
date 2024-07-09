"""
Tools for matching actual results against expected reference data.
"""

from typing import List, Optional

import numpy
import xarray


def compare_xarray(
    actual: xarray.DataArray,
    desired: xarray.DataArray,
    *,
    rtol: float = 1e-7,
    atol: float = 0,
    allowed_mismatch_fraction: float = 0,
    allowed_mismatch_rtol: Optional[float] = None,
    allowed_mismatch_atol: Optional[float] = None,
) -> List[str]:
    """
    Compare two xarray DataArrays with tolerance and report mismatch issues (as strings)

    Checks that are done (with tolerance):
    - (optional) Check fraction of mismatching pixels (difference exceeding some tolerance).
      If fraction is below a given threshold, ignore these mismatches in subsequent comparisons.
      If fraction is above the threshold, report this issue.
    - Compare actual and desired data with `numpy.testing.assert_allclose` and specified tolerances.

    :param actual: actual data
    :param desired: expected data
    :param rtol: relative tolerance
    :param atol: absolute tolerance
    :param allowed_mismatch_fraction: (optional) allowed fraction of mismatching elements to ignore
    :param allowed_mismatch_rtol: relative tolerance for mismatched elements to ignore
    :param allowed_mismatch_atol: absolute tolerance for mismatched elements to ignore
    :return: list of issues (empty if no issues)
    """
    assert isinstance(actual, xarray.DataArray)
    assert isinstance(desired, xarray.DataArray)
    # TODO: option for nodata fill value?
    # TODO: option to include data type check?
    # TODO: option to cast to some data type (or even rescale) before comparison?
    issues = []

    # TODO: first compare dims, coords, ... and finally abort with check on shape

    if actual.shape != desired.shape:
        issues.append(f"Shape mismatch: {actual.shape} != {desired.shape}")
        # Abort: no point in comparing data if shapes do not match
        return issues

    if allowed_mismatch_fraction:
        # Ignore limited fraction of mismatches
        if allowed_mismatch_rtol is None:
            allowed_mismatch_rtol = rtol
        if allowed_mismatch_atol is None:
            allowed_mismatch_atol = atol
        significantly_different = ~numpy.isclose(
            actual, desired, rtol=allowed_mismatch_rtol, atol=allowed_mismatch_atol, equal_nan=True
        )
        mismatch_fraction = significantly_different.mean()
        if mismatch_fraction > allowed_mismatch_fraction:
            issues.append(
                # TODO: include total counts in addition to fraction?
                f"Fraction of mismatched elements is too large: {mismatch_fraction:.2%} > {allowed_mismatch_fraction:.2%}"
            )
        else:
            # Ignore these mismatches in the following steps
            actual = actual.where(~significantly_different)
            desired = desired.where(~significantly_different)

    try:
        numpy.testing.assert_allclose(actual=actual, desired=desired, rtol=rtol, atol=atol, equal_nan=True)
    except AssertionError as e:
        # TODO: message of `assert_allclose` is typically multiline, split it again or make it one line?
        issues.append(str(e).strip())

    return issues
