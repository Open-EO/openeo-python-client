"""
HTML rendering helpers for metadata models (e.g. ``CubeMetadata``), used for Jupyter's ``_repr_html_``.

Kept in a separate module (instead of directly in ``openeo.metadata``) to keep the core metadata
data model free of rendering/presentation concerns.
"""

from __future__ import annotations

import html
from typing import List

from openeo.metadata import (
    BandDimension,
    Dimension,
    SpatialDimension,
    TemporalDimension,
)


def repr_html_dimensions(dimensions: List[Dimension]) -> str:
    """
    Render a compact HTML table with an overview of cube dimensions
    (name, type, size, labels/extent), based on available (client-side) metadata.
    """

    header = ["Dimension", "Type", "Size", "Labels / Extent"]
    rows = []
    for dim in dimensions:
        row = [dim.name, dim.type]
        if isinstance(dim, BandDimension):
            row.extend([str(len(dim.bands)), "bands: " + ", ".join(dim.band_names)])
        elif isinstance(dim, SpatialDimension):
            row.extend(["?", f"extent: {repr(dim.extent)}"])
        elif isinstance(dim, TemporalDimension):
            row.extend(["?", f"extent: {repr(dim.extent)}"])
        else:
            row.extend(["?", ""])
        rows.append("<tr>" + "".join(f"<td>{html.escape(c)}</td>" for c in row) + "</tr>")

    thead = "<tr>" + "".join(f"<th>{html.escape(c)}</th>" for c in header) + "</tr>"
    tbody = "".join(rows)

    return f"""<table>
            <thead>{thead}</thead>
            <tbody>{tbody}</tbody>
        </table>"""
