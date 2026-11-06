====================================
Spectral Indices
====================================

.. warning::
    This is a new experimental API, subject to change.

``openeo.extra.spectral_indices`` is an auxiliary subpackage
to simplify the calculation of common spectral indices
used in various Earth observation applications (vegetation, water, urban etc.).
It leverages the spectral indices defined in the
`Awesome Spectral Indices <https://awesome-ee-spectral-indices.readthedocs.io/>`_ project
by `David Montero Loaiza <https://github.com/davemlz>`_.

.. versionadded:: 0.9.1

Band mapping
=============

The formulas provided by "Awesome Spectral Indices" are defined in terms of standardized variable names
like "B" for blue, "R" for red, "N" for near-infrared, "WV" for water vapour, etc.

.. code-block:: json

       "NDVI": {
            "formula": "(N - R)/(N + R)",
            "long_name": "Normalized Difference Vegetation Index",

Obviously, these formula variables have to be mapped properly to the band names of your cube.

Automatic band mapping
-----------------------
In most simple cases, when there is enough collection metadata
to automatically detect the satellite platform (Sentinel2, Landsat8, ..)
and the original band names haven't been renamed,
this mapping will be handled automatically, e.g.:

.. code-block:: python
    :emphasize-lines: 2

    cube = connection.load_collection("SENTINEL2_L2A", ...)
    indices = compute_indices(cube, indices=["NDVI", "NDMI"])



.. _spectral_indices_manual_band_mapping:

Manual band mapping
--------------------

In more complex cases, it might be necessary to specify some additional information to guide the band mapping.
If the band names follow the standard, but it's just the satellite platform can not be guessed
from the collection metadata, it is typically enough to specify the platform explicitly:

.. code-block:: python
    :emphasize-lines: 4

    indices = compute_indices(
        cube,
        indices=["NDVI", "NDMI"],
        platform="SENTINEL2",
    )

Additionally, if the band names in your cube have been renamed, deviating from conventions, it is also
possible to explicitly specify the band name to spectral index variable name mapping:

.. code-block:: python
    :emphasize-lines: 4-8

    indices = compute_indices(
        cube,
        indices=["NDVI", "NDMI"],
        variable_map={
            "R": "S2-red",
            "N": "S2-nir",
            "S1": "S2-swir",
        },
    )

.. versionadded:: 0.26.0
    Function arguments ``platform`` and ``variable_map`` to fine-tune the band mapping.


API
====

.. automodule:: openeo.extra.spectral_indices
    :members: list_indices, compute_and_rescale_indices, append_and_rescale_indices, compute_indices, append_indices, compute_index, append_index
