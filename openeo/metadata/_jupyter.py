"""
HTML rendering helpers for metadata models (e.g. ``CubeMetadata``), used for Jupyter's ``_repr_html_``.

Kept in a separate module (instead of directly in ``openeo.metadata``) to keep the core metadata
data model free of rendering/presentation concerns.
"""

from __future__ import annotations

import functools
import html
from typing import List

from openeo.metadata import (
    BandDimension,
    Dimension,
    SpatialDimension,
    TemporalDimension,
)


def td(text: str, *, text_align: str | None = "left", escape: bool = True, element: str = "td") -> str:
    css = f"text-align: {text_align};" if text_align else ""
    attrs = f' style="{css}"' if css else ""
    if escape:
        text = html.escape(text)
    return f"<{element}{attrs}>{text}</{element}>"


th = functools.partial(td, element="th")


def repr_html_dimensions(dimensions: List[Dimension]) -> str:
    """
    Render a compact HTML table with an overview of cube dimensions
    (name, type, size, labels/extent), based on available (client-side) metadata.
    """
    header = ["Dimension", "Type", "Size", "Labels / Extent"]
    rows = []
    for dim in dimensions:
        row = [dim.name, str(dim.type)]
        if isinstance(dim, BandDimension):
            labels = ", ".join(str(name) if name else "?" for name in dim.band_names)
            row.extend([str(len(dim.bands)), "bands: " + labels])
        elif isinstance(dim, SpatialDimension):
            row.extend(["?", f"extent: {repr(dim.extent)}"])
        elif isinstance(dim, TemporalDimension):
            row.extend(["?", f"extent: {repr(dim.extent)}"])
        else:
            row.extend(["?", ""])
        rows.append("<tr>" + "".join(td(c) for c in row) + "</tr>")

    thead = "<tr>" + "".join(th(c) for c in header) + "</tr>"
    tbody = "".join(rows)

    return f"""<table>
            <thead>{thead}</thead>
            <tbody>{tbody}</tbody>
        </table>"""
