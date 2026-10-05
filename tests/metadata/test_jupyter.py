import re

import dirty_equals
import pytest

from openeo.metadata import (
    Band,
    BandDimension,
    GeometryDimension,
    SpatialDimension,
    TemporalDimension,
)
from openeo.metadata._jupyter import repr_html_dimensions


def _strip_inter_tag_whitespace(html: str) -> str:
    """
    Remove whitespace between HTML tags to simplify testing of HTML output.
    """
    html = html.strip()
    html = re.sub(r">\s+", ">", html)
    html = re.sub(r"\s+<", "<", html)
    return html


def test_strip_inter_tag_whitespace():
    assert (
        _strip_inter_tag_whitespace("\n<div>  <span> hello world\n</span> \n\n </div> \n")
        == "<div><span>hello world</span></div>"
    )


@pytest.mark.parametrize(
    ["dimension", "expected_body"],
    [
        (
            BandDimension(name="bands", bands=[Band("B2"), Band("B3")]),
            "<tr><td>bands</td><td>bands</td><td>2</td><td>bands: B2, B3</td></tr>",
        ),
        (
            SpatialDimension(name="y", extent=[50, 52]),
            "<tr><td>y</td><td>spatial</td><td>?</td><td>extent: [50, 52]</td></tr>",
        ),
        (
            TemporalDimension(name="t", extent=["2026-10-01", "2026-10-20"]),
            "<tr><td>t</td><td>temporal</td><td>?</td><td>extent: [&#x27;2026-10-01&#x27;, &#x27;2026-10-20&#x27;]</td></tr>",
        ),
    ],
)
def test_repr_html_dimensions_simple(dimension, expected_body):
    html = repr_html_dimensions([dimension])
    assert _strip_inter_tag_whitespace(html) == _strip_inter_tag_whitespace(
        f"""
        <table>
            <thead><tr><th>Dimension</th><th>Type</th><th>Size</th><th>Labels / Extent</th></tr></thead>
            <tbody>{expected_body}</tbody>
        </table>
        """
    )


def test_render_dimensions_html_basic():
    dimensions = [
        TemporalDimension(name="t", extent=["2026-10-01", "2026-11-12"]),
        BandDimension(name="bands", bands=[Band("B2"), Band("B3")]),
        SpatialDimension(name="x", extent=[2, 7]),
        SpatialDimension(name="y", extent=[49, 52]),
    ]
    html = repr_html_dimensions(dimensions)
    assert _strip_inter_tag_whitespace(html) == _strip_inter_tag_whitespace(
        """
            <table>
            <thead><tr><th>Dimension</th><th>Type</th><th>Size</th><th>Labels / Extent</th></tr></thead>
            <tbody>
                <tr><td>t</td><td>temporal</td><td>?</td><td>extent: [&#x27;2026-10-01&#x27;, &#x27;2026-11-12&#x27;]</td></tr>
                <tr><td>bands</td><td>bands</td><td>2</td><td>bands: B2, B3</td></tr>
                <tr><td>x</td><td>spatial</td><td>?</td><td>extent: [2, 7]</td></tr>
                <tr><td>y</td><td>spatial</td><td>?</td><td>extent: [49, 52]</td></tr>
            </tbody>
            </table>
        """
    )
