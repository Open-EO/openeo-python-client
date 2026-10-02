===============================================================
Organising and extracting reference data for machine learning
===============================================================

Training a machine learning (ML) model on Earth observation data starts with high-quality reference data: a set of
locations for which the target variable (e.g. a crop type, land cover class or biophysical measurement) is known. This
guide describes how to organise such reference data and how to extract the corresponding satellite observations with
openEO, either as **points** (tabular time series per sample) or as **patches** (small raster chips per sample).

Extraction modes
================

Depending on the type of ML model you are training, you will want one of two extraction modes:

.. list-table::
   :header-rows: 1
   :widths: 15 15 35 35

   * - Mode
     - openEO process
     - Typical model
     - Output
   * - **Point extraction**
     - ``aggregate_spatial``
     - Models operating on per-pixel (time series) inputs, such as random forests, gradient boosting or time series
       deep learning models (e.g. LSTM, transformers)
     - A vector cube, exported as tabular data (Parquet/CSV) with one row per sample and observation date
   * - **Patch extraction**
     - ``filter_spatial``
     - Models requiring spatial context, such as convolutional neural networks or vision transformers
     - A sparse raster cube, exported as one netCDF or GeoTIFF file per sample polygon

Organising your reference data
==============================

Format and structure
--------------------

Reference data is best managed as a vector dataset, with one feature per sample:

* **Geometry**: a point for point extractions, or a polygon (the patch outline) for patch extractions. For patch
  extractions, keep patches small, e.g. 64x64 up to 512x512 pixels; larger areas are better handled as a separate job
  per sample.
* **Target attribute(s)**: the label or value that your model will learn to predict.
* **Sample identifier**: a unique ID per sample, so extracted pixels can be traced back to the original reference data.
* **Temporal information** (recommended): a validity date or period per sample, so you can define the correct temporal
  extent when extracting satellite data.

The recommended file formats are **GeoParquet** and **GeoJSON**. GeoParquet is preferred for larger datasets, as it is
compressed, columnar and faster to read.

.. tip::
    The `WorldCereal Reference Data Module (RDM) <https://rdm.esa-worldcereal.org/>`_ is a good example of how
    reference data can be curated and harmonised at scale: samples are stored with a unique ID, a harmonised label,
    a validity time and quality attributes, and are exported as GeoParquet for extraction.

Cataloguing with STAC
---------------------

When your reference data grows beyond a handful of files, the `STAC <https://stacspec.org/>`_ specification can help
to keep it organised and discoverable: describe each GeoParquet/GeoJSON file as a STAC item, with its spatial and
temporal extent as item metadata and the file itself as an asset, and group the items in a STAC collection. The
`label extension <https://github.com/stac-extensions/label>`_ can additionally document the classes and tasks of your
labels. This makes it easy to search reference data by region or period, and the asset URLs can be passed directly to
``load_url`` for extraction.

Hosting the data at a URL
-------------------------

Sending many geometries inline as GeoJSON quickly makes openEO requests too large. Instead, upload your reference data
to a (publicly) accessible URL, for example object storage (S3), `Artifactory <https://jfrog.com/artifactory/>`_ or a
GitHub repository, and load it into openEO with the ``load_url`` process
(:py:meth:`Connection.load_url() <openeo.rest.connection.Connection.load_url>`):

.. code-block:: python

    import openeo

    connection = openeo.connect("openeofed.dataspace.copernicus.eu").authenticate_oidc()

    reference_data = connection.load_url(
        "https://example.com/my_reference_data.parquet",
        format="Parquet",
    )

For GeoJSON files, use ``format="GeoJSON"`` instead.

If you do not have your own hosting, the openEO Python client provides an
:doc:`artifact helper <../api-artifacts>` that temporarily stores files on
S3 storage offered by the backend and returns a presigned URL that can be passed straight to ``load_url``:

.. code-block:: python

    from openeo.extra.artifacts import build_artifact_helper

    artifact_helper = build_artifact_helper(connection)
    storage_uri = artifact_helper.upload_file("my_reference_data.parquet")
    reference_data_url = artifact_helper.get_presigned_url(storage_uri)

    reference_data = connection.load_url(reference_data_url, format="Parquet")

.. note::
    Artifact storage is backend-specific functionality and the uploaded files are only kept temporarily, so it is well
    suited for one-off extractions, but not as a long-term home for your reference dataset.

.. _ml-splitting-spatial-groups:

Splitting into spatial groups
-----------------------------

For large-scale (e.g. continental or global) reference datasets, do not extract everything in a single batch job.
Instead, split your samples into spatial groups, resulting in one GeoParquet/GeoJSON file and one openEO batch job per
group. The **Sentinel-2 tiling grid** (MGRS, ~110x110 km tiles) is a good unit for this: one job per Sentinel-2 tile
keeps the job size manageable, aligns with how the input data is stored, and guarantees that each group stays within a
single UTM zone.

The openEO Python client offers a
:ref:`job splitting utility <job-splitting>`
(:py:func:`~openeo.extra.job_management._job_splitting.split_area` in ``openeo.extra.job_management``) that partitions
your area or samples over a tile grid, such as a
pre-computed Sentinel-2 tile grid, and returns a ``GeoDataFrame`` with one row per tile — a ready-made starting point
for a job database. The openEO :ref:`MultiBackendJobManager <job-manager>`
can then run and track the resulting list of extraction jobs.

.. important::
    Not only the spatial extent of a group, but also its **spatial density** — how many reference observations fall
    within that extent — determines how large a job can be. A tile with thousands of samples may need to be split
    further, while sparsely sampled tiles could be merged into a single job. Finding the right job size may require
    some trial-and-error finetuning.

Loading satellite data
======================

Load the satellite collections that provide the input features of your model, restricting the spatial and temporal
extent to what is needed for your samples. A typical multi-sensor example with Sentinel-2 optical data and Sentinel-1
SAR data:

.. code-block:: python

    temporal_extent = ["2024-01-01", "2024-12-31"]

    s2 = connection.load_collection(
        "SENTINEL2_L2A",
        temporal_extent=temporal_extent,
        bands=["B02", "B03", "B04", "B08", "B11", "B12", "SCL"],
        max_cloud_cover=80,
    )

    s1 = connection.load_collection(
        "SENTINEL1_GRD",
        temporal_extent=temporal_extent,
        bands=["VV", "VH"],
    )

There is no need to specify a ``spatial_extent``: the extraction processes (``aggregate_spatial`` or
``filter_spatial``) will restrict the computation to the sample locations.

Preprocessing
=============

Whether and where to preprocess is a design choice of your ML pipeline:

* **Preprocess in openEO**: apply cloud masking (e.g. based on the Sentinel-2 ``SCL`` band with
  ``mask_scl_dilation``/``to_scl_dilation_mask``), compute backscatter with ``sar_backscatter``, composite to regular
  time steps with ``aggregate_temporal_period``, resample with ``resample_cube_spatial`` and merge sensors with
  ``merge_cubes``. This yields analysis-ready training data and keeps the ML code simple.
* **Preprocess in the model**: extract (nearly) raw observations and perform normalisation, compositing or feature
  engineering inside the model itself, e.g. as part of an `ONNX <https://onnx.ai/>`_ model that is later used for
  inference in openEO. This guarantees that training and inference apply identical preprocessing.

Whichever option you choose, make sure the extraction produces exactly the inputs your model expects at inference time.

.. code-block:: python

    # Example: monthly compositing and merging into a single feature cube
    s2_masked = s2.process(
        "mask_scl_dilation",
        data=s2,
        scl_band_name="SCL",
    )
    s2_monthly = s2_masked.aggregate_temporal_period(period="month", reducer="median")
    s1_monthly = s1.aggregate_temporal_period(period="month", reducer="mean")

    features = s2_monthly.merge_cubes(s1_monthly.resample_cube_spatial(s2_monthly))

Point extraction with ``aggregate_spatial``
===========================================

For point-based models, use :py:meth:`~openeo.rest.datacube.DataCube.aggregate_spatial` to sample the feature cube at
your reference locations. The reducer receives the pixel values intersecting each geometry. For point geometries this
is typically a single pixel, so the choice of reducer barely matters: any reducer (``mean``, ``median``, ``first``,
...) will simply return that pixel's value.

``aggregate_spatial`` can also be used with polygon geometries, e.g. to extract one aggregated time series per parcel
instead of per pixel. In that case the reducer does matter, as it determines how the pixels within each polygon are
combined (e.g. ``mean`` or ``median``).

.. code-block:: python

    samples = features.aggregate_spatial(
        geometries=reference_data,
        reducer="mean",
    )

    job = samples.execute_batch(
        out_format="Parquet",
        title="ML point extraction",
    )

In contrast to ``filter_spatial``, which produces raster output, ``aggregate_spatial`` produces **vector output**: a
table with one value per sample, band and timestep, which can be exported to GeoParquet or CSV and used directly for
model training.

.. admonition:: Attribute propagation
    :class: tip

    All attributes (properties) of the features in your reference data — labels, sample identifiers, validity dates —
    are propagated to the extracted output, both in the GeoParquet output of ``aggregate_spatial`` and in the netCDF
    output of ``filter_spatial``. There is no need to join the extractions back to the reference data afterwards:
    labels and metadata travel along with the extracted pixel values.

Patch extraction with ``filter_spatial``
========================================

For models that need spatial context, use :py:meth:`~openeo.rest.datacube.DataCube.filter_spatial` with polygon
geometries describing the patch outlines. The backend then only computes the data cube for those polygons, producing a
sparse **raster** output cube. Combine this with the ``sample_by_feature`` output option to write one file per sample:

.. code-block:: python

    patches = features.filter_spatial(geometries=reference_data)

    job = patches.create_job(
        title="ML patch extraction",
        out_format="netCDF",
        sample_by_feature=True,
        feature_id_property="sample_id",
    )
    job.start_and_wait()
    job.get_results().download_files("patches/")

Each resulting netCDF file contains the full time series of all bands for one patch, named after the ``sample_id``
property of the corresponding feature. These files can be converted into training tensors for your deep learning
framework of choice.

.. warning::
    Extraction only works as a **batch job**, since it produces multiple output files that cannot be transferred in a
    synchronous call.

Performance considerations
==========================

* Extraction is not necessarily cheap: even a sparse output may require reading a large number of raw EO assets. The
  input data volume, not the output size, usually determines cost and duration.
* Group samples spatially (see :ref:`above <ml-splitting-spatial-groups>`) and run one job per Sentinel-2 tile.
* Restrict the temporal extent and band selection to what your model actually needs.
* Consider caching extractions: store the extracted training data (e.g. as GeoParquet in object storage) so that model
  iterations do not require re-extraction.
