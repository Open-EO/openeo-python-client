# Geospatial AI Playbook

A hands-on guide to incorporating machine-learning projects with openEO end-to-end, from reference data and feature
extraction to training, model packaging, inference, validation, and upscaling.

This playbook focuses on what works in real projects and where teams usually lose time.

It complements {doc}`Machine Learning </machine_learning>`, {doc}`User-Defined Functions </udf>`,
{doc}`User-Defined Processes </udp>`, and {doc}`Batch Jobs </batch_jobs>`.

````{admonition} Quickstart: what is where
:class: tip

| If you want to... | Go to |
| :--- | :--- |
| Understand where openEO fits in an ML workflow | [Where openEO helps most](#where-openeo-helps-most-in-a-machine-learning-ml-workflow) |
| Extract features with an openEO graph | [1. Data extraction and feature engineering](#1-data-extraction-and-feature-engineering-in-openeo) |
| Organise reference data, sample, split, host it at a URL | [2. Reference data](#2-organising-and-extracting-reference-data-for-ml) |
| Extract points or patches, deliver to storage | [2.5 Point extraction with `aggregate_spatial`](#25-point-extraction-with-aggregate_spatial), [2.6 Patch extraction with `filter_spatial`](#26-patch-extraction-with-filter_spatial) |
| Decide where to train, or train lightweight models in openEO | [3. Training strategy](#3-training-strategy-where-openeo-fits) |
| Choose between UDF and `predict_onnx` | [4.0 Choosing your onboarding path](#40-choosing-your-onboarding-path-udfs-and-predict_onnx) |
| Export a model to ONNX | [4.1 Preparing the model](#41-preparing-the-model-onnx-export-and-calibration) |
| Deploy a model | [4.2 UDF](#42-deploying-via-user-defined-functions), [4.3 `predict_onnx`](#43-deploying-via-predict_onnx) |
| Describe a model with STAC | [5. ML models and STAC](#5-ml-models-and-stac) |
| Run inference reliably inside a UDF | [7. Running inference inside the UDF](#7-running-inference-inside-the-udf) |
| Validate a model and benchmark it | [8. Validation pipelines](#8-validation-pipelines-in-openeo) |
| Scale up, tune job options, control cost | [9.1 Scaling](#91-incremental-scaling-strategy), [9.2 Job-option tuning](#92-job-option-tuning), [9.3 Cost](#93-cost-and-credit-control) |

**Scope labels.** Statements are valid on all openEO backends unless marked *CDSE-specific*, *Terrascope-specific*, or
*backend-specific*. "Verified 2026-11-23" means checked against the live capabilities of `openeo.dataspace.copernicus.eu`
(CDSE) and `openeo.vito.be` (Terrascope).

**Check the backend before generating code.** Process and runtime support differs per backend:

```python
import openeo

connection = openeo.connect("openeofed.dataspace.copernicus.eu").authenticate_oidc()

needed = {"load_url", "aggregate_spatial", "filter_spatial", "predict_onnx", "export_workspace", "run_udf"}
print("missing processes:", sorted(needed - {p["id"] for p in connection.list_processes()}))
print("UDF runtimes:", list(connection.list_udf_runtimes()))
print("output formats:", list(connection.list_output_formats()))
```

Verified 2026-11-23: all processes above are listed on both CDSE and Terrascope.

**Job options policy.** `job_options` keys and values are backend-specific. Read the target backend's documentation or
ask the user; never copy values from the examples here unchanged.
````

## Where openEO helps most in a Machine Learning (ML) workflow

openEO is strongest at consistent, reproducible EO data access and processing at scale. In practice, a robust setup uses
openEO for feature extraction and operational inference, while heavy training often stays in external GPU environments.
Once trained, the model is brought back into openEO as a reusable inference block.

This split is usually a strength: extraction and inference stay reproducible, while model development stays flexible. By
keeping openEO constraints in mind from the start, many later operational hurdles can be avoided.

This playbook shares the working patterns we have learned from real projects. Its scope covers:

* organising reference data with STAC,
* extracting EO features,
* training models, converting them to ONNX,
* describing models with STAC,
* running inference and validation pipelines in openEO, and
* upscaling with performance tuning.

Each of those stages maps to a section below.

## End-to-end story

```{image} /_static/images/geospatial-ai-workflow.svg
:alt: End-to-end geospatial AI workflow
:width: 100%
:align: center
```

## 1. Data extraction and feature engineering in openEO

If your goal is an ML model that is eventually operationalized through openEO, start with openEO constraints in mind from
day one. The biggest failures we see are not model or openEO failures; they are data-distribution failures: training on
data that is not available on target backends, or training on a product variant that differs from what operations will use
(for example, processing-baseline differences).

### 1.1 Start with backend discovery

The first real step is backend discovery, not model design. Before you write a single line of model code, check
which collections are actually available on the backend you intend to use in production, whether their temporal coverage is
good enough for your use case, and whether the backend processing level matches your planned production workflow. A model
trained on Sentinel-2 L2A data from one backend may not behave the same way when the operational backend serves a different
processing baseline, a different cloud-masking approach, or a different atmospheric correction. These differences are often
subtle in isolation but compound across a full inference run.

No single backend has everything. When a needed source is missing, openEO's `load_stac` is often the clean fallback path
for integrating external, open, STAC-compliant assets (see {py:meth}`~openeo.rest.connection.Connection.load_stac`). This lets you
pull in data from other catalogs without leaving the openEO graph, which keeps the extraction logic in one place rather
than splitting it across systems.

### 1.2 Make the data contract explicit

The practical goal is simple: make feature extraction an openEO-native graph from day one so that the model trained on
those features still behaves correctly during openEO inference. Once extraction is reproducible and composable, you can
turn it into integration and regression checks (for example, in ESA APEx: https://apex.esa.int/algorithm-support). That
gives you a clear 'MLOps' path and a way to share preprocessing pipelines with the community.

In this way, you maintain an explicit data-input pipeline that is transparent about input bands and source collections,
physical units and scaling (for example, reflectance in `[0, 1]`, DN, SAR dB vs linear), CRS and resolution, temporal
compositing, cloud/no-data handling, and feature-engineering steps. Each of these choices affects what the model sees, and
making them explicit rather than implicit is what allows anyone to reproduce a data extraction.

### 1.3 Freeze and version the extraction graph

When moving toward the training step, freeze the extraction graph and treat it as part of the model definition. This means
versioning the graph alongside the model weights, so that when you later load the model for inference, you can verify that
the input features are being generated by the exact same logic that produced the training data.

A practical way to do this is to publish the extraction graph as a parameterised {doc}`user-defined process </udp>` with an
explicit version, and record that version with every job. When the processing logic changes, produce new outputs under the
new version and mark the old ones as superseded instead of overwriting them. Part of the provenance comes for free: on
backends that expose STAC metadata for batch job results, you can capture the input products and the process graph that
produced them, so keep that metadata together with the extracted training data.

### 1.4 Example: a simple feature extraction graph

```python
import openeo

connection = openeo.connect("openeofed.dataspace.copernicus.eu").authenticate_oidc()

spatial_extent = {"west": 5.08, "south": 51.20, "east": 5.22, "north": 51.31}
temporal_extent = ["2024-04-01", "2024-09-30"]

cube = connection.load_collection(
    "SENTINEL2_L2A",
    spatial_extent=spatial_extent,
    temporal_extent=temporal_extent,
    bands=["B02", "B03", "B04", "B08"],
    max_cloud_cover=80,
)

# Simple cloud mask: SCL classes 3 (cloud shadow), 8-9 (cloud) and 10 (cirrus).
scl = connection.load_collection(
    "SENTINEL2_L2A",
    spatial_extent=spatial_extent,
    temporal_extent=temporal_extent,
    bands=["SCL"],
    max_cloud_cover=80,
).band("SCL")
cloud_mask = (scl == 3) | (scl == 8) | (scl == 9) | (scl == 10)

features = cube.mask(cloud_mask).aggregate_temporal_period(period="dekad", reducer="median")

job = features.execute_batch(
    out_format="NetCDF",
    title="features_s2_dekad_v1",
)
```

In the next section, we will go deeper into how to optimally arrange the data extraction.

## 2. Organising and extracting reference data for ML

Training a machine-learning model on Earth observation data starts with high-quality reference data: a set of locations for
which the target variable (for example, a crop type, land-cover class, or biophysical measurement) is known. This section
describes how to organise such reference data and how to extract the corresponding satellite observations with openEO,
either as points (tabular time series per sample) or as patches (small raster chips per sample).
For a worked end-to-end example, see the [extracting-training-data
notebook](https://github.com/Open-EO/openeo-community-examples/tree/main/python/40_machine-learning/extracting-training-data)
(Sentinel-2 L2A with LUCAS 2022 reference samples).

### 2.1 Extraction modes

The first decision you make is whether your model needs per-pixel time series or spatial patches, dictated by the
architecture of the model you intend to train. A random forest or gradient-boosted tree expects a table of features per
sample, while a convolutional neural network expects a small image chip around each sample.
openEO supports both, but the extraction process, the output format, and the job-sizing considerations are different enough
that it is worth deciding early.

- Point extraction
  Process: `aggregate_spatial`
  Typical model: models operating on per-pixel time-series inputs, such as random forests,
  gradient boosting, or time-series deep-learning models (for example, LSTM, transformers)
  Output: a vector cube, exported as tabular data (Parquet/CSV) with one row per sample and
  observation date
- Patch extraction
  Process: `filter_spatial`
  Typical model: models requiring spatial context, such as convolutional neural networks or
  vision transformers
  Output: a sparse raster cube, exported as one netCDF or GeoTIFF file per sample polygon

For process details and parameters, see
{py:meth}`~openeo.rest.datacube.DataCube.aggregate_spatial`
and
{py:meth}`~openeo.rest.datacube.DataCube.filter_spatial`.

The output format follows from the extraction mode and from how you consume the result:

| Format | Type | Typical use |
| :--- | :--- | :--- |
| GeoTIFF (cloud-optimized) | raster | Patches that should open in any GIS or web viewer. |
| netCDF | raster | A full time series per patch in a single file; common for deep-learning training. |
| Zarr (experimental) | raster | Large arrays for cloud-native analysis. |
| (Geo)Parquet | vector | Point extractions with many samples; retains feature attributes. |
| GeoJSON | vector | Small extractions that you want to inspect by hand. |
| CSV / JSON | vector | Quick inspection in spreadsheets; may not retain feature attributes. |

Supported formats and their options are backend-specific; check them with
{py:meth}`~openeo.rest.connection.Connection.list_file_formats`.

### 2.2 Organising your reference data

#### Format and structure

Reference data is best managed as a vector dataset, with one feature per sample. The structure you choose here determines
how smoothly the rest of the pipeline runs, because every downstream step (extraction, training, validation) inherits the
decisions you make at this stage.

- **Geometry**: a point for point extractions, or a polygon (the patch outline) for patch extractions. For patch
  extractions, keep patches small, for example 64x64 up to 512x512 pixels; larger areas are better handled as a separate
  job per sample. The reason for this limit is practical: a single large polygon forces the backend to materialize a large
  raster cube for that one sample, which can dominate job cost and memory even when the rest of the samples are tiny.
  Derive patch polygons from a fixed, pixel-aligned grid (for example a UTM-based grid at the target resolution) rather
  than buffering arbitrary points, so that patches from different sensors and different runs line up pixel by pixel.
- **Target attribute(s)**: the label or value that your model will learn to predict. If you have multiple target variables,
  keep them as separate columns rather than encoding them into a single field; this makes it easier to train different
  models from the same reference dataset without re-extracting.
- **Sample identifier**: a unique ID per sample, so extracted pixels can be traced back to the original reference data.
  This becomes essential during validation, when you need to compare predictions against reference labels sample by sample.
  Keep the ID stable when a label or geometry is corrected later, so earlier extractions remain traceable.
- **Temporal information (recommended)**: a validity date or period per sample, so you can define the correct temporal
  extent when extracting satellite data. Without this, you either extract a fixed window for all samples (which may be
  wrong for samples collected in different seasons) or you manually split the reference data into temporal groups, which is
  error-prone at scale.

The recommended file formats are GeoParquet and GeoJSON. GeoParquet is preferred for larger datasets, as it is compressed,
columnar, and faster to read. GeoJSON is fine for small datasets and for cases where you need to inspect the file by hand,
but it becomes unwieldy beyond a few thousand features.

```{tip}
The WorldCereal Reference Data Module (RDM) is a good example of how reference data can be curated and harmonised at
scale: samples are stored with a unique ID, a harmonised label, a validity time, and quality attributes, and are exported
as GeoParquet for extraction.
```

#### Label quality

Reference labels can be wrong, so treat their quality as explicit metadata rather than an assumption. A simple controlled
vocabulary at dataset level works well, for example:

- `unverified`: delivered as-is by the provider;
- `automated`: passed automated checks (geometry validity, schema, duplicates, class consistency);
- `reviewed`: a sample was checked by an interpreter, with the reported sample size and estimated label accuracy;
- `validated`: every feature was independently verified.

Where available, add per-feature columns such as `qa_status`, `qa_method`, `qa_date`, and `qa_confidence`. Because
attributes travel with the extracted values (see [Attribute propagation](#attribute-propagation)), you can then filter or
weight samples by label quality at training time without re-extracting.

#### Cataloguing with STAC

When your reference data grows beyond a handful of files, the STAC specification can help keep it organised and
discoverable. A structure that scales well is one STAC collection per reference dataset (identity, description, licence,
provider, label schema, and overall extent) and one STAC item per published release of that dataset, with the
GeoParquet/GeoJSON file(s) as assets. Corrections and additions become a new release, with a new item and new, immutable
file paths, so a training run can always refer to the exact release it used. Use the STAC time fields for the period in
which the reference observations are valid (`datetime`, or `start_datetime`/`end_datetime`), not for the upload date. The
label extension can additionally document the classes and tasks of your labels. This makes it easy to search reference data by region or period, and the asset URLs can be passed to
`load_url` for extraction. The same applies to the extracted training data: where supported, `export_workspace` can
write batch job results, with their STAC metadata, directly into a STAC API (see
[Delivering patches to storage](#delivering-patches-to-storage)).

The practical benefit is that you stop tracking reference data in a spreadsheet or a folder tree. Instead, you can query a
catalog for "all reference data covering this region and this time period" and get back a list of URLs that feed directly
into the extraction pipeline. This is especially valuable when reference data is contributed by multiple partners or
collected over several years.

#### Sampling strategy

Which samples you extract determines both the cost and the quality of the model. A random or regular selection over all
available reference data is usually dominated by common classes and spatially clustered locations. A strategy that works
well in practice:

1. **Start from the approved reference data.** Exclude or down-weight samples with insufficient label quality (see
   [Label quality](#label-quality)).
2. **Stratify by class** and, where spatial variability matters, also by zone (for example ecoregion, climate zone, or
   country). Rare classes can be merged into a parent class when too few samples exist.
3. **Allocate a target number of samples per stratum.** Common rules are equal allocation (balanced classification), a
   minimum per class with the remainder proportional to class frequency, a compromise proportional to class frequency
   raised to a power between 0 and 1, or explicit quotas. Strata with fewer candidates than their target are taken in full
   and reported as under-represented.
4. **Spread samples spatially within each stratum**, for example with spatially balanced sampling (GRTS), a minimum
   separation distance, or quotas per coarse grid cell, to avoid clusters.
5. **Account for patches with multiple classes.** A patch selected for one class also contains others: count classes from
   the actual patch content, and fill rare classes first.
6. **Report before extracting.** Check per-class and per-zone counts, class balance, and spatial coverage before spending
   compute on extraction.

Make the selection reproducible: use a fixed random seed, a versioned strategy, and a stable ordering (for example a hash
of seed and sample ID), so that new reference data can be added later without reshuffling the existing selection.

#### Spatially independent data splits

Neighbouring samples are strongly correlated. If samples from the same field or the same few kilometres end up in both the
training and the test set, accuracy estimates will be too optimistic. Assign each sample to a spatial block, taken from a
coarse grid that is larger than the expected autocorrelation range (often tens of kilometres), and put whole blocks into
the train, validation, or test split, stratified so that every split contains all classes where possible. Store the split
as an attribute of the reference data rather than deriving it at training time, and keep a block in the same split when
new data is added.

#### Hosting the data at a URL

Sending many geometries inline as GeoJSON quickly makes openEO requests too large. Instead, upload your reference data to a
URL the backend can reach, for example object storage (S3), Artifactory, or a GitHub repository, and load it into openEO
with the `load_url` process
({py:meth}`Connection.load_url() <openeo.rest.connection.Connection.load_url>`):

```python
import openeo

connection = openeo.connect("openeofed.dataspace.copernicus.eu").authenticate_oidc()

reference_data = connection.load_url(
    "https://example.com/my_reference_data.parquet",
    format="Parquet",
)
```

For GeoJSON files, use `format="GeoJSON"` instead.

Not all reference data can be public: labels are often licensed or restricted. In that case, keep the files private and
pass a short-lived signed (presigned) URL to `load_url`. Store the stable, unsigned location in your catalogue, never the
signed URL itself, and generate a fresh signed URL when you submit an extraction job: a job resubmitted with an expired URL
will fail.

If you do not have your own hosting, the openEO Python client provides an {doc}`artifact helper </api-artifacts>` that
temporarily stores files on S3 storage offered by the backend and returns a presigned URL that can be passed straight to
`load_url`:

```python
from openeo.extra.artifacts import build_artifact_helper

artifact_helper = build_artifact_helper(connection)
storage_uri = artifact_helper.upload_file("my_reference_data.parquet")
reference_data_url = artifact_helper.get_presigned_url(storage_uri)

reference_data = connection.load_url(reference_data_url, format="Parquet")
```

**CDSE-specific.** The artifact helper is **backend-specific** functionality and the uploaded files are only kept
temporarily, so it is well suited for one-off extractions, but not as a long-term home for your reference dataset.
By default, the presigned URL returned by `get_presigned_url` is valid for 7 days. If you rely on it for a
multi-week training campaign, you may find that your reference data has expired between runs.

#### Splitting into spatial groups

For large-scale (for example, continental or global) reference datasets, do not extract everything in a single batch
job. The most fundamental reason is projection: a job that crosses UTM zones has to reproject part of the data, so
samples in different zones no longer line up with the native pixel grid of the input data. A second reason is request
size and memory pressure: very large AOIs can exceed backend limits or run out of memory before completion. A third
reason is job duration: a job that covers e.g. an entire country runs for so long that any transient failure forces
you to start over. Instead, split your samples into spatial groups, resulting in one GeoParquet/GeoJSON file and one
openEO batch job per group. The Sentinel-2 tiling grid (MGRS, roughly 110x110 km tiles) is a good unit for this: one
job per Sentinel-2 tile guarantees that each group stays within a single UTM zone, keeps the job size manageable, and
aligns with how the input data is stored.

The openEO Python client offers a job splitting utility
(the {py:func}`~openeo.extra.job_management.split_area` function, see {ref}`job-splitting-custom-grid`) that partitions your area or
samples over a tile grid, such as a pre-computed Sentinel-2 tile grid, and returns a GeoDataFrame with one row per tile — a
ready-made starting point for a job database. The openEO {py:class}`~openeo.extra.job_management.MultiBackendJobManager` (see {ref}`job-manager`) can
then run and track the resulting list of extraction jobs. For patch extractions, note that size-based splitting does not
guarantee pixel alignment: pass a pre-computed grid, and derive the patch polygons from that same pixel-aligned grid.

In `split_area`, these two modes are mutually exclusive: either pass `projection` + `tile_size` for size-based tiling,
or pass `tile_grid` for a pre-defined grid. In size-based mode, tiles are anchored at the AOI south-west corner and edge
tiles are clipped to the AOI boundary; this is convenient for load balancing but not automatically pixel-aligned.

In addition the `group_geometries` helper can be used to pre-batch very large
feature sets into manageable geometry groups before launching extraction jobs. 

```python
# group many input features into small batches before extraction.
try:
  from openeo.extra.job_management import group_geometries

  groups = group_geometries(reference_data, max_geometries=500)
except ImportError:
  # Fallback on older client versions: build groups manually.
  groups = [reference_data.iloc[i:i + 500] for i in range(0, len(reference_data), 500)]
```

Use these groups as your per-job input geometries, optionally combined with `split_area` for large AOIs.

```{important}
Not only the spatial extent of a group, but also its spatial density (how many reference observations fall within that
extent) determines how large a job can be. A tile with thousands of samples may need to be split further, while sparsely
sampled tiles could be merged into a single job. Finding the right job size may require some trial-and-error fine-tuning.
A good starting point is to aim for a few hundred to a few thousand samples per job, then adjust based on observed
runtime and memory usage.
```

### 2.3 Loading satellite data

Load the satellite collections that provide the input features of your model, restricting the spatial and temporal extent
to what is needed for your samples. A typical multi-sensor example with Sentinel-2 optical data and Sentinel-1 GRD (SAR)
data:

```python
temporal_extent = ["2024-01-01", "2024-12-31"]

s2 = connection.load_collection(
    "SENTINEL2_L2A",
    temporal_extent=temporal_extent,
    bands=["B02", "B03", "B04", "B08", "B11", "B12"],
    max_cloud_cover=80,
)
s1 = connection.load_collection(
    "SENTINEL1_GRD",
    temporal_extent=temporal_extent,
    bands=["VV", "VH"],
)
```

At this point, there is no need to specify a `spatial_extent`: the extraction processes referenced above
(`aggregate_spatial` or `filter_spatial`) come into play at the end of the process graph and restrict the computation to
the sample locations.


### 2.4 Preprocessing

Whether and where to preprocess is a design choice of your ML pipeline, and it is worth making it explicitly rather than
letting it emerge by accident. The two options have different trade-offs:

- **Preprocess in openEO**: apply cloud masking (for example, a binary mask derived from the Sentinel-2 SCL band),
  compute backscatter with {py:meth}`~openeo.rest.datacube.DataCube.sar_backscatter`, composite to regular time steps
  with {py:meth}`~openeo.rest.datacube.DataCube.aggregate_temporal_period`, and merge sensors with {py:meth}`~openeo.rest.datacube.DataCube.merge_cubes`. This
  yields analysis-ready training data and keeps the ML code simple. The advantage is that the preprocessing is visible in
  the openEO graph, versioned alongside the extraction logic, and applied identically to training and inference.
- **Preprocess in the model**: extract (nearly) raw observations and perform normalisation, compositing, or feature
  engineering inside the model itself, for example as part of an ONNX model that is later used for inference in openEO.
  This guarantees that training and inference apply identical preprocessing, but it pushes complexity into the model export
  and makes the preprocessing harder to inspect or modify without retraining.

Whichever option you choose, make sure the extraction produces exactly the inputs your model expects at inference time. The
most common source of silent accuracy loss in production is a mismatch between the preprocessing applied during training
and the preprocessing applied during inference; for example, training on reflectance scaled to `[0, 1]` and inferring on DN
values, or training with cloud-masked composites and inferring on raw scenes.

```python
# Example: simple SCL cloud masking, monthly compositing and merging into a single feature cube
cloud_mask = (scl == 3) | (scl == 8) | (scl == 9) | (scl == 10)
s2_monthly = s2.mask(cloud_mask).aggregate_temporal_period(period="month", reducer="median")
s1_monthly = s1.aggregate_temporal_period(period="month", reducer="mean")

features = s2_monthly.merge_cubes(s1_monthly)
```

Related community example: [basic-sentinel-merge](https://github.com/Open-EO/openeo-community-examples/tree/main/python/30_openeo-processing/basic-sentinel-merge)
for merging Sentinel-1 and Sentinel-2 into one cube.

### 2.5 Point extraction with `aggregate_spatial`

For point-based models, use
{py:meth}`~openeo.rest.datacube.DataCube.aggregate_spatial`
to sample the feature cube at your reference locations. The reducer receives the pixel values intersecting each geometry.
For point geometries this is typically a single pixel, so the choice of reducer barely matters: any reducer (`mean`,
`median`, `first`, ...) will simply return that pixel's value.

`aggregate_spatial` can also be used with polygon geometries, for example to extract one aggregated time series per parcel
instead of per pixel. In that case the reducer does matter, as it determines how the pixels within each polygon are
combined (for example, `mean` or `median`).

```python
samples = features.aggregate_spatial(
    geometries=reference_data,
    reducer="mean",
)

job = samples.execute_batch(
    out_format="Parquet",
    title="ML point extraction",
)
```

In contrast to `filter_spatial`, which produces raster output, `aggregate_spatial` produces vector output: a table with one
value per sample, band, and timestep, which can be exported to GeoParquet or CSV and used directly for model training. This
tabular structure is what makes point extraction a natural fit for classical ML models, which expect a fixed-length feature
vector per sample.

### 2.6 Patch extraction with `filter_spatial`

For models that need spatial context, use
{py:meth}`~openeo.rest.datacube.DataCube.filter_spatial`
with polygon geometries describing the patch outlines. The backend then only computes the data cube for those polygons,
producing a sparse raster output cube. Combine this with the **backend-specific** `sample_by_feature` output option
to write one file per sample (see
{py:meth}`DataCube.create_job() <openeo.rest.datacube.DataCube.create_job>` and {doc}`sampling`):

```python
patches = features.filter_spatial(geometries=reference_data)

job = patches.create_job(
    title="ML patch extraction",
    out_format="netCDF",
    sample_by_feature=True,
    feature_id_property="sample_id",
)
job.start_and_wait()
job.get_results().download_files("patches/")
```

Without `sample_by_feature`, the backend typically writes the whole sparse cube as one output, mostly empty space between the
patches. With `sample_by_feature=True`, it writes a separate file per feature of the `filter_spatial` geometries, each
covering only that feature's extent. The output format options on CDSE and Terrascope are:

| Option | Default | Effect |
| :--- | :--- | :--- |
| `sample_by_feature` | `false` | netCDF: one file per feature, containing all bands and dates. GTiff: one file per feature *and* date. |
| `feature_id_property` | `null` | Name of the feature attribute used as feature ID in the file names, to link each output back to its input feature. Without it, the backend chooses the names, so set it to keep outputs traceable. |
| `filename_prefix` | `null` | Prefix for the output file names (by default `OpenEO` or part of the input file name). |

So in the example above, each netCDF file contains the full time series of all bands for one patch, named after the
`sample_id` of the corresponding feature. Use a unique, stable ID column (see
[Format and structure](#format-and-structure)). These files can be converted into training tensors for your deep learning
framework of choice.

These are **backend-specific** format options: check them with
{py:meth}`~openeo.rest.connection.Connection.list_file_formats` (under `output` → format → `parameters`) for your backend.

#### Attribute propagation

Depending on the backend, all attributes (properties) of the features in your reference data (labels, sample identifiers,
validity dates) are propagated to the extracted output. For `aggregate_spatial`, these properties are carried into the
vector result. For `filter_spatial`, the feature metadata is retained in per-feature outputs when using
`sample_by_feature` together with a stable `feature_id_property`. In the common workflow, there is no need to join
extractions back to the reference data afterwards: labels and metadata travel with the extracted values. This saves a
surprising amount of error-prone bookkeeping, especially when reference data is split across multiple files and
extraction jobs.

```{tip}
Always validate on a small test extraction if all original feature properties are retained before relying on it.
```

```{warning}
Extraction only works as a batch job, since it produces multiple output files that cannot be transferred in a synchronous
call.
```

#### Delivering patches to storage

{py:meth}`download_files <openeo.rest.job.JobResults.download_files>` is convenient for experiments, but batch job results are only kept for a limited time. For larger or
recurring extractions, let the backend write the results straight to your own object storage with {py:meth}`~openeo.rest.stac_resource.StacResource.export_workspace`
(where supported), which also writes the STAC metadata. On backends that support direct STAC API publishing for
workspaces, this is usually the preferred path: results become queryable in a STAC catalog immediately, without
download/re-upload loops.

```python
result = patches.save_result(
    format="netCDF",
    # backend-specific format options
    options={"sample_by_feature": True, "feature_id_property": "sample_id"},
).export_workspace(workspace="my_workspace")

job = result.create_job(title="ML patch extraction")
```

Which workspaces are available, and whether you can register your own storage, is backend-specific. When running such
extractions through the job manager, create it with `download_results=False` (see {ref}`job-manager-download-results`), so results are not downloaded a second time.
Check the backend process listing (for example `/openeo/1.2/processes`) to confirm `export_workspace` availability and
workspace options.



### 2.7 Performance considerations

Extraction is not necessarily cheap: even a sparse output may require reading a large number of raw EO assets. The input
data volume, not the output size, usually determines cost and duration. A patch extraction that produces a few megabytes of
netCDF output may still read gigabytes of Sentinel-2 imagery to compute those patches, because the backend has to load
and process multiple chunks from each scene that intersects the sample locations.

- Group samples spatially (see [Splitting into spatial groups](#splitting-into-spatial-groups)) and run one job per
  Sentinel-2 tile.
- Run a small pilot extraction first, and use its observed cost per sample to estimate the cost of the full extraction (see
  [9.3](#93-cost-and-credit-control)).
- Restrict the temporal extent and band selection to what your model actually needs. Every extra band and every extra month
  of data adds to the input volume, even if the model ignores it.
- Consider caching extractions: store the extracted training data (for example, as GeoParquet in object storage) so that
  model iterations do not require re-extraction. This is especially valuable during the experimentation phase, when you may
  retrain a model dozens of times with different hyperparameters but the same input features.

## 3. Training strategy: where openEO fits

### 3.1 Training outside openEO (common path)

Deep learning training usually runs outside openEO on GPU-capable environments. Which environment fits depends on what you
actually need:

- **Existing training datasets.** If your task overlaps with a published EO training dataset, reusing it can remove a
  large part of the reference-data work described in [Section 2](#2-organising-and-extracting-reference-data-for-ml).
  Datasets are shared through several channels, for example [Source Cooperative](https://source.coop/),
  [Hugging Face](https://huggingface.co/datasets), the dataset loaders of [TorchGeo](https://torchgeo.readthedocs.io/),
  or [EOTDL](https://www.eotdl.com/) (Earth Observation Training Data Lab). Pin the exact dataset version you train on.
- **GPU compute.** Options range from local or institutional GPU machines and HPC centres to commercial clouds and
  EO-specific platforms that offer GPU workspaces (EOTDL is one such European option). GPU training is the piece openEO
  deliberately does not try to cover.

The reason training typically happens outside openEO is not a limitation of the platform but a matter of fit: openEO is
optimized for scalable EO data processing, not for GPU-accelerated gradient descent, and the tooling around model
development (experiment tracking, hyperparameter search, GPU scheduling) is more mature elsewhere.

The important part for this playbook is not where training runs, but that training sees exactly the same feature semantics
as operational inference. If you train on a laptop with locally downloaded data and then deploy to openEO, you are
implicitly trusting that the local data and the openEO data are equivalent. That trust is often misplaced, because the two
paths may differ in cloud masking, atmospheric correction, resampling, or compositing. The same holds for third-party
training datasets: whatever dataset you train on, the features must match what your openEO inference graph will produce.
The safest route is to extract the training data with the same openEO graph used for inference (see
[Section 1](#1-data-extraction-and-feature-engineering-in-openeo)).

### 3.2 Training in openEO for lightweight models

Reference implementation used here: [random-forest forest-fire
example](https://github.com/Open-EO/openeo-community-examples/tree/main/python/40_machine-learning/random-forest-forest-fire).

For tree-based models, openEO does support native training and inference, given that it is genuinely CPU-friendly:
{py:meth}`~openeo.rest.vectorcube.VectorCube.fit_class_random_forest` trains server-side on the backend's regular compute nodes. You send openEO a cube
of input features (predictors) and a set of labelled points (y_train), and the backend fits the
trees.

In that reference workflow, `training_cube` is built by merging the cubes produced by the `s1_features` (Sentinel-1) and
`s2_features` (Sentinel-2) helper functions before calling `aggregate_spatial`. This keeps training and inference aligned
because the same feature-building helpers are reused for prediction:

```python
s1_feature_cube = s1_features(connection, temporal_extent, spatial_extent, "median")
s2_feature_cube = s2_features(connection, temporal_extent, spatial_extent, "median", padding_window_size)
training_cube = s2_feature_cube.merge_cubes(s1_feature_cube)
```

```python
import json

# y_train: GeoDataFrame with labelled training points (geometry + "target" column)
target = json.loads(y_train.to_json())
predictors = training_cube.aggregate_spatial(target, reducer="median")
model = predictors.fit_class_random_forest(
    target=target,
    num_trees=200,
    seed=2082,
)
# Save the model as a batch job result asset
# so that we can load it in another job.
model = model.save_ml_model()
job = model.create_job(title="random_forest_training")
job.start_and_wait()
```

`fit_class_random_forest` trains directly on the backend against those aggregated predictors and the `target` labels.

When saving the model, it does not return to your laptop; instead, the batch job persists the fitted model as a
result asset, alongside the usual STAC metadata.

### 3.3 Using the trained model at inference time

The companion notebook, [`random-forest-inference-udp.ipynb`](https://github.com/Open-EO/openeo-community-examples/tree/main/python/40_machine-learning/random-forest-forest-fire), picks the model up purely by URL:

```python
# link to the trained model
model = "https://s3.waw3-1.cloudferro.com/apex-examples/RF-ForestFire/RandomForest-ForestFire-model.json"
```

We can then reuse the same `s1_features` / `s2_features` helper functions to build the inference cube; there is no risk of
training/serving skew from re-implementing feature logic twice:

```python
# Load s1 and s2 features
s1_feature_cube = s1_features(connection, temporal_extent, spatial_extent, "median")
s2_feature_cube = s2_features(connection, temporal_extent, spatial_extent, "median", padding_window_size)
# Merge the two feature cubes
inference_cube = s2_feature_cube.merge_cubes(s1_feature_cube)

# predict on new data
inference = inference_cube.predict_random_forest(
    model=model,
    dimension="bands",
)
```

If your model cannot be expressed as a random forest (or another `predict_*` process openEO ships natively), fall back to
the ONNX/UDF path described below.

### 3.4 Embedding-first strategy

While at first glance CPU-friendly support seems limiting, it opens interesting alternative options in today's new paradigm with
embeddings.

Embeddings are "general" features learned from multi-modal satellite data. Such pre-computed or on-demand computed
features, based on advanced ML architectures, can already provide the heavy lifting for many applications. Instead of
training a large model from scratch for each new task, you can use an embedding model to convert raw imagery into a compact
representation, then train a small, task-specific model on top of that representation. This is often both cheaper and more
accurate than training a task-specific model end-to-end, because the embedding model has already learned to capture
relevant spatial and spectral patterns from a much larger and more diverse dataset.

Hence, in combination with such embeddings, CPU-friendly training methodologies become a cost-efficient way of getting to
specific insights. A random forest or logistic regression on top of embeddings can be trained in minutes on a laptop, while
still benefiting from the representational power of a large backbone.

Within openEO, it is perfectly possible to generate embeddings on the fly as part of the feature-extraction pipeline. This
means you do not need to pre-compute and store embeddings for your entire area of interest; instead, the embedding model
runs as part of the same graph that produces your training features and your inference outputs, which guarantees
consistency between the two.

A practical pattern is:

1. Compute embeddings once in openEO.
2. Train small downstream models on top.
3. Deploy those light models back into openEO.

In the next few sections, we detail how more heavyweight models, such as embedders, can be converted and integrated into openEO.
While the reference material focuses on embeddings, similar methods have been used to integrate alternative AI model workflows into openEO.

Reference: [geospatial embeddings
examples](https://github.com/Open-EO/openeo-community-examples/tree/main/python/60_geospatial-embeddings). This folder is
the practical starting point for the embedding-first pattern. It contains several worked examples that cover different
points on the spectrum from classical dimensionality reduction to foundation-model-style embeddings:

- **[Dimensionality reduction](https://github.com/Open-EO/openeo-community-examples/tree/main/python/60_geospatial-embeddings/dimensionality-reduction)**,
  a PCA-based UDF showing how a compact representation can be derived without a heavy backbone, useful as a baseline
  before committing to a large model.
- **[CORSA](https://github.com/Open-EO/openeo-community-examples/tree/main/python/60_geospatial-embeddings/corsa)**,
  learned compression of Sentinel-2 through custom backend processes.
- **[TESSERA](https://github.com/Open-EO/openeo-community-examples/tree/main/python/60_geospatial-embeddings/tessera)**
  (PyTorch UDF) and **[TerraMind](https://github.com/Open-EO/openeo-community-examples/tree/main/python/60_geospatial-embeddings/terramind)**
  (ONNX UDF), which demonstrate foundation-model-style embeddings. TerraMind also shows how the input calibration the
  network depends on is baked into the exported ONNX graph (see [Section 4.1.2](#412-calibration-lives-inside-the-model)).
- **[OlmoEarth](https://github.com/Open-EO/openeo-community-examples/tree/main/python/60_geospatial-embeddings/olmoearth)**,
  which computes annual Sentinel-1/2 embeddings with native `predict_onnx`: no UDF, no dependency archive and no GPU.

Once an embedding model is chosen, its pipeline is fixed: a pinned checkpoint, a known input layout and no custom
logic between the bands and the model. That makes `predict_onnx` (see [4.3](#43-deploying-via-predict_onnx)) usually
a better fit than a UDF. Reserve UDFs for the experimental phase, or for steps that native processes cannot express.

Each example is a runnable notebook rather than a fragment, so it doubles as a template for your own pipeline.

## 4. Packaging and deploying models for openEO inference

This section covers the two things that happen between "the model is trained" and "the model runs in an openEO job": preparing a deployable artifact, and choosing how that artifact is served by the backend. The first is largely shared across deployment paths; the second is where the two onboarding options diverge.

### 4.0 Choosing your onboarding path: UDFs and `predict_onnx`

openEO currently offers two  mechanisms for running ML inference, each with a different balance of flexibility, operational simplicity, and portability. This section compares them so you can choose the right tool for your use case rather than defaulting to the most familiar one.

At a glance:

| Criterion | UDF (User-Defined Function) | `predict_onnx` (native process) |
| :--- | :--- | :--- |
| **Flexibility** | High: arbitrary Python code, full `xarray` access, `context` object | Fixed: operates on a tensor interface, no `context` or band-name access |
| **Operational simplicity** | Medium: you manage the UDF wrapper and dependencies | High: no UDF code, backend handles ONNX runtime |
| **Portability** | Medium to high: basic `xarray`/NumPy code is very portable across versions; requires backends to implement `run_udf` | Medium: ONNX is portable, but process support is backend-specific |
| **Dependency management** | High: Inline in the UDF header (PEP 723) or via dependency archives for ML frameworks (PyTorch, TensorFlow) | None: the ONNX runtime is fixed by the backend |
| **Performance** | Medium: Depends largely on the quality of the user code | High: Backend can optimise generistic execution  |
| **Best for** | Custom preprocessing/postprocessing or custom libraries | Fixed per-pixel/per-tile ONNX models on supported backends |



#### 4.0.1 User-Defined Functions (UDFs)

UDFs are the most flexible and established way to run custom ML code in openEO. The `run_udf` process executes arbitrary Python code on the backend, receiving the data cube as an `xarray.DataArray`.

**Advantages:**

- **Maximum flexibility:** you can implement any preprocessing, model inference, and postprocessing logic in Python, including custom neural network architectures and specialised embedding models.
- **Full data cube access:** the UDF receives band names, coordinates, and the `context` object, allowing you to pass per-job parameters and inspect the cube's structure.
- **Mature ecosystem:** well-documented patterns exist for managing dependencies, either inline in the UDF header (see {ref}`python-udf-dependency-declaration`) or via `udf-dependency-archives`, and for applying UDFs with {py:meth}`~openeo.rest.datacube.DataCube.apply`, {py:meth}`~openeo.rest.datacube.DataCube.apply_dimension`, and {py:meth}`~openeo.rest.datacube.DataCube.apply_neighborhood`.

**Disadvantages:**
- **Dependency management:** for heavy ML frameworks (for example PyTorch or TensorFlow) you are responsible for building and shipping dependency archives, which can be large and increase cold-start times.
- **Performance and memory tuning:** pre-defined processes are typically better optimized by the backend; with a UDF, the quality of the user code matters more. Because the backend has no good way to predict the memory needs of custom code, you will usually need to fine-tune the memory settings of UDF jobs.

**Best for:** any model that requires custom Python logic, access to the full data cube (band names, coordinates), or per-job parameterisation via the `context` object. See [Section 4.2](#42-deploying-via-user-defined-functions) for the operational details.

Try to do as much pre- or postprocessing with pre-defined processes before resorting to a UDF, and keep UDFs small and modular to minimise dependencies. Nevertheless, do avoid chaning multiple UDF executions after each other. Typically its more efficient to perform the user-defined code within a single run_udf process.

```{tip}
Even inside a UDF, it is best practice to run inference on an ONNX-converted model with `onnxruntime` rather than loading the full training framework (PyTorch, TensorFlow). ONNX Runtime has a much smaller memory footprint and dependency archive, which reduces out-of-memory errors and cold-start times.

This also makes the UDF a natural precursor to `predict_onnx`: the UDF gives you room to experiment, add pre- and postprocessing code around the inference call, and pull in extra dependencies. Once the workflow is stable and the extra logic can be moved into native openEO processes (or into the ONNX graph itself), switch to `predict_onnx` with the same ONNX model.
```

#### 4.0.2 Native `predict_onnx` Process [experimental]

`predict_onnx` is a more streamlined, backend-native approach for deploying ONNX models. Instead of writing a UDF wrapper, you pass the model directly to the process, and the backend handles the ONNX runtime and data chunking internally.

**Advantages:**

- **No UDF code required:** you avoid writing and maintaining a UDF wrapper, which simplifies the operational pipeline.
- **Simplified dependency management:** the backend provides and fixes the ONNX runtime; there is no dependency archive to manage.
- **Performance:** because the backend controls the full execution, it can go far in optimising inference performance.
- **Precision and testability:** you can test the exact ONNX graph locally with `onnxruntime` to verify parity before running a job, giving high confidence in the results.

**Disadvantages:**

- **Experimental, check your backend:** treat `predict_onnx` as experimental. It is listed on CDSE and Terrascope (verified 2026-11-23), but other backends may not offer it; check {py:meth}`connection.list_processes() <openeo.rest.connection.Connection.list_processes>`. The CDSE community example is the reference implementation. 
- **Fixed interface:** the process operates on a tensor interface with no access to band names, coordinates, or the `context` object, making it unsuitable for models that need this metadata.
- **Limited to ONNX:** only models that can be exported to ONNX and conform to the expected input/output contract are supported.

**Best for:** fixed, per-pixel or per-tile computations where the model is a self-contained ONNX graph and the target backend supports the process. See [Section 4.3](#43-deploying-via-predict_onnx) for a deployment example.

#### 4.0.3 Decision Guide

```{image} /_static/images/geospatial-ai-inference-path.svg
:alt: Decision guide: predict_onnx for fixed ONNX models without custom logic when the backend lists predict_onnx; otherwise a UDF with ONNX Runtime.
:width: 100%
:align: center
```

Use the following rules of thumb to choose the right onboarding path:

1. **Use `predict_onnx`** if your model is a fixed, per-pixel ONNX graph and the target backend supports the process. It is the simplest operational path, with no UDF wrapper and minimal dependency management.
2. **Use a UDF** for everything else that requires custom Python code, the `context` object, or access to the full data cube (band names, coordinates). This remains the most flexible and widely applicable option. Keep the UDF small, push as much preprocessing as possible into native openEO processes, and watch memory usage. Prefer an ONNX-converted model inside the UDF for its low memory footprint; this also makes the UDF a good sandbox to iterate in before moving to `predict_onnx`.

This decision framework complements the STAC-based model description discussed in [Section 5](#5-ml-models-and-stac): by describing your model with MLM metadata, you make it discoverable regardless of which onboarding path you ultimately choose.

```{note}
The two options are not mutually exclusive. One can perfectly combine UDFs with predict_onnx into a single pipeline if need be.
```

**Where to go next**

- **`predict_onnx`:** follow [4.1 Preparing the model](#41-preparing-the-model-onnx-export-and-calibration), then [4.3 Deploying via `predict_onnx`](#43-deploying-via-predict_onnx).
- **UDF:** follow [4.1 Preparing the model](#41-preparing-the-model-onnx-export-and-calibration), then [4.2 Deploying via User-Defined Functions](#42-deploying-via-user-defined-functions). Sections 7 to 9 then go deep on UDF inference patterns, validation, and upscaling.

The rest of this playbook (Sections 7 to 9) goes deep on the UDF path, because it is the most flexible and the most widely applicable option today. `predict_onnx` readers should use the short deployment section below together with the linked external documentation.

### 4.1 Preparing the model: ONNX export and calibration

Whatever deployment path you choose, the artifact that actually runs in the backend is in practice almost always an ONNX model. `predict_onnx` consumes it directly and a UDF typically loads it via ONNX Runtime. The preparation steps in this subsection therefore apply to both paths, and getting them right early saves debugging effort later.

#### 4.1.1 Prefer ONNX when feasible

Default to ONNX export for stable inference models.

In practice, this usually wins because it is lighter and simpler to operate. You generally get a faster startup and less
dependency pain than shipping a full deep-learning framework. An ONNX Runtime archive is on the order of tens of megabytes,
while a full PyTorch archive can be hundreds of megabytes, and that difference translates directly into cold-start time and
memory overhead for every worker that loads the archive.

Use full-framework deployment only when you really need it: custom operators that do not export, highly dynamic model
logic, or models that are still changing every week.

#### 4.1.2 Calibration lives inside the model

An added benefit of ONNX is that the framework provides more than a CPU-friendly transition: it is flexible enough to embed
the input calibration the network depends on (for example, z-score normalization), so the exported graph stays
self-contained instead of relying on the UDF to reproduce preprocessing exactly.

```python
import torch
from torch import nn


class EncoderONNX(nn.Module):
    def __init__(self, backbone, mean, std):
        super().__init__()
        self.backbone = backbone
        self.register_buffer("mean", torch.tensor(mean).view(1, -1, 1, 1))
        self.register_buffer("std", torch.tensor(std).view(1, -1, 1, 1))

    def forward(self, data):
        return self.backbone((data - self.mean) / (self.std + 1e-9))
```

In this way you can be certain that the correct data distribution enters the model at runtime, whether it is invoked via `predict_onnx` or from inside a UDF.

#### 4.1.3 Tips for the ONNX conversion

For general conversion guides, see the [ONNX tutorials](https://onnx.ai/onnx/intro/converters.html) and the
framework-specific exporters, such as [`torch.onnx`](https://docs.pytorch.org/docs/stable/onnx.html) for PyTorch,
[tf2onnx](https://github.com/onnx/tensorflow-onnx) for TensorFlow/Keras, and
[skl2onnx](https://onnx.ai/sklearn-onnx/) for scikit-learn.

Using ONNX is a strong choice when all of these are true:

1. The architecture is mostly fixed.
2. You can define one stable input/output contract (shape, dtype, layout).
3. You can pass source-vs-ONNX parity on representative data.
4. The target backend/runtime can run the exported opset with acceptable latency.

When exporting a model, treat export as an interface contract and lock the following parameters
explicitly:

1. **Input signature**: names, dtypes, rank, layout (`NCHW`/`NHWC`), dynamic axes. Dynamic axes can be a great asset for
   openEO, because optimization often means making the model run over as large an area as possible in one go. If the batch
   dimension is dynamic, the same exported model can process one tile or many tiles without re-export.
2. **Opset version**: explicit and pinned, never implicit defaults. Different opset versions support different operators,
   and an opset that works on your local ONNX Runtime may not be supported by the runtime available in the openEO backend.
3. **Output semantics**: logits/probabilities/embeddings and exact expected shape. This matters most when the output is
   consumed by downstream code that assumes a particular interpretation; a model that outputs logits but is treated as
   probabilities will produce subtly wrong results.
4. **Runtime target**: test on the runtime you will actually use in production (usually ONNX Runtime CPU in openEO UDF
   jobs). Testing on a different runtime, or on a GPU, can hide issues that only appear on the production runtime.

**Reference export pipeline (PyTorch)**

```python
import torch

# 0) Wrap the trained backbone so calibration is part of the exported graph (see 4.1.2).
wrapped = EncoderONNX(backbone, mean, std)

# 1) Build a realistic dummy input in the exact layout your runtime will receive.
dummy = torch.randn(1, 4, 224, 224, dtype=torch.float32)

# 2) Export with explicit names, dynamic axes, and pinned opset.
torch.onnx.export(
    wrapped.eval().cpu(),
    dummy,
    "encoder.onnx",
    input_names=["data"],
    output_names=["embedding"],
    dynamic_axes={"data": {0: "batch"}, "embedding": {0: "batch"}},
    opset_version=17,
)
```

**Sanity checks**

Once the model is ported to ONNX and you want to incorporate the inference into an openEO job, there are a few safety
checks one has to build in. When openEO passes the input cube to the model, it is not guaranteed that the order of bands,
dimensions, or dtype of the cube is as you expected. Whether you consume the model via `predict_onnx` or from inside a
UDF, it is good practice to ensure there is no:

1. **Layout mismatch**: trained with `NCHW`, inferred with `NHWC`. This is the most common mistake, because your
   conventions and openEO cube conventions are not the same, and the transpose may be implicit in one path but not the
   other.

2. **Implicit preprocessing**: training normalised input, but the exported graph assumes raw data. If normalisation is not baked
   into e.g. the exported ONNX graph, the UDF must reproduce it exactly, and any deviation will degrade accuracy without raising an
   error.

3. **dtype drift**: training path used `uint16`, runtime standardizes on float32.

4. **Dynamic-axis mismatch**: batch dynamic but spatial dimensions accidentally fixed (or vice versa). A model exported
   with a fixed spatial size will fail when during runtime you provide a different tile size.

### 4.2 Deploying via User-Defined Functions

The most common way to run a custom ML model inside openEO is a {doc}`User-Defined Function
(UDF) </udf>`, simply because it offers incredible flexibility: a UDF
lets you import arbitrary Python code into your process graph instead of being limited to what the backend ships natively.
This is what makes it possible to run a custom neural network, a specialized embedding model, or any other Python-based
inference logic inside an openEO job.

That flexibility comes with a catch: UDF runtimes in the cloud are intentionally minimal. They ship with a small set of
pre-installed packages, and they are designed to be lightweight so that jobs can start quickly and run
efficiently on shared infrastructure. You can inspect what is available with the [runtime
docs](https://openeo.dataspace.copernicus.eu/openeo/1.2/udf_runtimes) or, on an established connection, with:

```python
connection.list_udf_runtimes()
```

```{note}
UDF runtimes and package support are backend-specific. Always check
`connection.list_udf_runtimes()` (or the backend runtime endpoint) before pinning dependency
archives, and retest when runtime versions change.
```

However, there are two ways to extend what a UDF can use:

1. **Small packages**: install them inline inside the UDF itself via the PEP 723 "inline script metadata"
   standard (see {ref}`python-udf-dependency-declaration`). This is the lightest
   option and works well for pure-Python dependencies. It is best suited for small utilities that do not have compiled
   extensions or complex transitive dependencies.
2. **Heavier packages** (the common case for ML): ship a prebuilt dependency archive and attach it through [job
   options](https://documentation.dataspace.copernicus.eu/APIs/openEO/job_config.html). Building and maintaining such
   archives requires dedicated tooling and solid openEO/runtime knowledge, so we build and maintain ready-to-use archives
   ourselves for the common ML formats (ONNX, PyTorch). The archive is downloaded once per worker and unpacked into the UDF
   runtime, which is why keeping it small matters for cold-start time.

A good strategy order is: start with pre-built dependency archives, then use small runtime additions for tiny extras, and
keep heavy runtime installation as a last resort.

For a model exported to ONNX (see [4.1](#41-preparing-the-model-onnx-export-and-calibration)), the UDF only needs
[ONNX Runtime](https://onnxruntime.ai/docs/), not the full training framework. Pre-built ONNX Runtime dependency archives
are available, for example:

```text
https://s3.waw3-1.cloudferro.com/project_dependencies/onnx_deps_python311.zip#onnx_deps
```

Example of wiring a dependency archive through job options:

```python
# `pred` is the inference cube built with a UDF, see Section 7.1
job = pred.execute_batch(
    out_format="NetCDF",
    title="udf_inference_with_deps",
    job_options={
        "udf-dependency-archives": [
            "https://example.org/archives/onnxruntime_deps.zip#deps"
        ],
        "executor-memory": "3g",
        "max-executors": 12,
    },
)
```

```{note}
`job_options` are backend-specific: both supported keys and optimal values (for example, scheduler/executor tuning)
depend on the concrete backend that executes the job.

Take extra care on federation and multi-backend endpoints: jobs are routed to a concrete backend, and only that backend's
supported options are applied. Always check the target backend documentation/capabilities, keep a small
backend-specific profile, and validate with a tiny smoke run before scaling.
```

The syntax matters here because the archive is mounted into the UDF runtime at a specific path, and the `#` fragment in the
URL is what names that path. A model file and its runtime dependencies are typically passed as separate archives with
separate mount points, so the UDF can refer to them by a stable path regardless of the worker it happens to run on:

```python
job_options={
    "udf-dependency-archives": [
        "https://example.org/archives/onnxruntime_deps.zip#deps",
        "https://example.org/archives/model_files.zip#model",
    ],
}
```

Inside the UDF, those archives are then available as `deps/` and `model/` relative to the UDF's working directory.
A typical UDF bootstrap pattern is:

```python
import sys

# Keep default module resolution first; add mounted dependencies as fallback.
sys.path.append("deps")

import onnx
```

Using a ready-made archive like this is exactly the "start with pre-built dependency archives" strategy recommended above:
it removes the build step from your critical path and lets you focus on the model itself.

Related UDF examples: [onnx-inference](https://github.com/Open-EO/openeo-community-examples/tree/main/python/40_machine-learning/onnx-inference)
runs a pretrained CNN with ONNX Runtime inside a UDF, and
[parcel-delineation](https://github.com/Open-EO/openeo-community-examples/tree/main/python/40_machine-learning/parcel-delineation)
deploys a pretrained U-Net.

See [Section 7](#7-running-inference-inside-the-udf) for the patterns that make UDF inference reliable at scale: how to
choose between `apply`, `apply_dimension`, and `apply_neighborhood`, how to handle chunk padding, and how to cache
sessions and tune thread counts.

### 4.3 Deploying via `predict_onnx`

Once an ONNX artifact is exported and parity-tested (see [4.1](#41-preparing-the-model-onnx-export-and-calibration)), `predict_onnx` ({py:func}`openeo.processes.predict_onnx`) is the simplest deployment path on backends that support it. The process takes the model (URL) and the array of values to predict on, typically applied along a dimension of the cube, and the backend handles the ONNX runtime, session caching, and chunking.

```python
from openeo.processes import predict_onnx

prediction = cube.apply_dimension(
    dimension="bands",
    process=lambda bands: predict_onnx(data=bands, model="https://example.org/encoder.onnx"),
)
```

Three things to watch:

- **Contract, not code.** The input cube must already match the model's expected shape, dtype, and band order. There is no `context` and no band-name access inside the process, so any preprocessing must be done with native openEO processes (or baked into the ONNX graph, see [4.1.2](#412-calibration-lives-inside-the-model)) before the call.
- **Backend support.** The process is experimental. It is listed on CDSE and Terrascope (verified 2026-11-23) but may be missing elsewhere; check `connection.list_processes()` before designing around it.
- **Spatial context.** For CNN-style models, check how the backend tiles the input. According to the community examples, the CDSE `predict_onnx` has no overlap option, so tiles are processed without overlap and seams are possible (the OlmoEarth example below accepts this). If your model needs overlapping windows, use a UDF with `apply_neighborhood` instead (see [4.2](#42-deploying-via-user-defined-functions)).

The [OlmoEarth example](https://github.com/Open-EO/openeo-community-examples/tree/main/python/60_geospatial-embeddings/olmoearth) shows the full pattern for an embedding model:

- the model is exported and parity-checked locally;
- the monthly Sentinel-1/2 inputs are built with native processes;
- the ONNX file is uploaded as an artifact and run through `predict_onnx`, with a STAC model description.

For simple per-pixel or per-tile models, `predict_onnx` is usually less code and less operational overhead than the equivalent UDF. For anything that needs custom logic around the model, fall back to Section 4.2.

## 5. ML models and STAC

In many projects we have seen that a single global model rarely works equally well everywhere, so the metadata
around a model matters almost as much as the model itself: it tells a future user, including future you, where the model is
expected to work and under which exact input contract.

The STAC community originally proposes the [Machine Learning Model (MLM) extension](https://github.com/stac-extensions/mlm), which
adds rich support for things like multiple model inputs, hyperparameters, and accelerator requirements.

At minimum, capture the spatiotemporal relevance, input band order, expected units and value ranges, tensor layout, expected tile shape and window
semantics, the normalization policy, output semantics, runtime requirements, and links to all sidecar
artifacts. This kind of information is especially relevant during deployment.

```json
{
  "stac_extensions": [
    "https://stac-extensions.github.io/mlm/v1.4.0/schema.json"
  ],
  "properties": {
    "mlm:name": "crop-encoder-v3",
    "mlm:architecture": "resnet-encoder",
    "mlm:tasks": ["classification"],
    "apex:input_bands": ["B02", "B03", "B04", "B08"],
    "apex:input_units": "surface_reflectance_0_1",
    "apex:input_layout": "NCHW",
    "apex:tile_shape": [224, 224],
    "apex:normalization": "embedded_in_onnx",
    "apex:output": {"name": "embedding", "dimensions": ["batch", 768]},
    "apex:onnx_opset": 17
  },
  "assets": {
    "model": {"href": "https://example.org/model.onnx", "type": "application/onnx"},
    "weights": {"href": "https://example.org/model.onnx.data", "roles": ["data"]}
  }
}
```

## Related references

- {doc}`Machine Learning </machine_learning>`
- {doc}`User-Defined Functions </udf>`
- {doc}`User-Defined Processes </udp>`
- {doc}`Batch Jobs </batch_jobs>`
- {doc}`Data Access </data_access>`
- [openEO community examples: geospatial embeddings](https://github.com/Open-EO/openeo-community-examples/tree/main/python/60_geospatial-embeddings)
- [openEO community examples: machine learning](https://github.com/Open-EO/openeo-community-examples/tree/main/python/40_machine-learning)

## 6. Emerging: a standardized ML API in openEO

Everything in this playbook currently relies on two mechanisms for getting a trained model into openEO: a `predict_*`
process that the backend ships natively (as in the forest-fire example), or a UDF that loads the model itself. Both work
today, and both are what you should build on for production right now.

There is, however, ongoing work to standardize how models are described and loaded in openEO, and it is worth being aware
of it so you can follow the direction of travel rather than being surprised by it later. The idea is to treat a trained
model as a first-class, discoverable object rather than as a file behind a URL:

- Models are described as STAC Items using the [MLM extension](https://github.com/stac-extensions/mlm), which is the same
  metadata layer discussed in [Section 5](#5-ml-models-and-stac).
- A dedicated process retrieves such a model directly from a STAC catalog, so the model URL, its input contract, and its
  provenance travel together.
- Inference then runs against that loaded model, without the user having to wire up a dependency archive or a UDF by hand
  for the common case.

This is a natural extension of the STAC description work already recommended in Section 5: if you have gone to the trouble
of describing your model with MLM metadata, a standardized loader is what makes that metadata pay off by letting other jobs
find and use the model directly. It does not change any of the practical advice in this playbook — keep using UDFs and
dependency archives for anything non-standard — but it is the direction the ecosystem is moving, and describing your models
with MLM metadata now positions them to take advantage of it when it lands.

## 7. Running inference inside the UDF

With the model packaged and described, the remaining work happens inside the UDF itself: deciding how the datacube is
chunked for the model, keeping sessions and sockets alive across invocations, and making sure a window of pixels is handled
consistently from training all the way through to a large-scale production run. The next few sections walk through the
patterns that tend to make this reliable in practice.

### 7.1 Choosing between `apply`, `apply_dimension`, and `apply_neighborhood`

openEO offers several ways of applying your custom script along a datacube, and the one you want depends mainly on the
input your ML network actually needs. Choosing the wrong one is not just inefficient; it can produce incorrect results if
the model expects spatial context that the process does not provide.

Quick rule of thumb:

1. Use [`apply`](https://openeo.org/documentation/1.0/processes.html#apply) for lightweight element-wise transforms
  (for example scaling or thresholding) where you do not need to control chunking explicitly.
2. Use [`apply_dimension`](https://openeo.org/documentation/1.0/processes.html#apply_dimension) when logic runs along one
   dimension (most often time, `t`) for each pixel location. This is the natural choice for time-series models that consume
   a full temporal profile per pixel.
3. Use [`apply_neighborhood`](https://openeo.org/documentation/1.0/processes.html#apply_neighborhood) for model inference
  whenever you need explicit control over spatial and temporal chunking, including many pixel-based models. It is required
  for patch-based models (for example CNN-style inference), and it can also be preferable for pixel-based inference because
  you define chunk sizes and overlap yourself instead of relying on backend defaults.

Minimal patterns:

```python
# 1) Lightweight element-wise transform: no custom chunking needed.
scaled = prediction.apply(lambda x: x * 100.0)
```

```python
# 2) Per-pixel time-series logic along one dimension.
udf_temporal = openeo.UDF.from_file("udf_temporal.py")

temporal_out = cube.apply_dimension(
    dimension="t",
    process=udf_temporal,
)
```

```python
# 3) Spatial patch inference with overlap.
udf_spatial = openeo.UDF.from_file("udf_spatial.py")

pred = features.apply_neighborhood(
    process=udf_spatial,
    size=[
        {"dimension": "x", "value": 128, "unit": "px"},
        {"dimension": "y", "value":   128, "unit": "px"},
        {"dimension": "t", "value": 'P1D'}
    ],
    overlap=[
        {"dimension": "x", "value": 8, "unit": "px"},
        {"dimension": "y", "value": 8, "unit": "px"},
    ],
)
```

### 7.2 Spatial context and chunk semantics

For patch-based models that go through `apply_neighborhood`, the window size and overlap you choose at inference time need
to match what the model actually learned during training. The official process guidance is a useful anchor here: spatial
window sizes typically range from 64 to 512 pixels, with overlaps of 8 to 32 pixels being common, and openEO will stitch
the kept centers of neighboring chunks back together once processing finishes. A larger overlap reduces edge artifacts but
also increases memory pressure, since every extra pixel of overlap is recomputed in both of its neighboring chunks.

If the entire AOI is not an exact multiple of the apply window, center padding with NaNs is applied during the UDF runtime.
Therefore it is paramount that the UDF implementation knows how to work with NaN inputs. A UDF that assumes all input
pixels are valid will either crash or produce garbage at the edges of the AOI, and those errors can be difficult to trace
back to the padding behavior.

### 7.3 Caching and concurrency

A single batch job can spin up many workers, and each of those workers may process several chunks over its lifetime.
Loading a model and spinning up an inference session is comparatively expensive, so it pays off to cache the loaded
artifact at the module level instead of re-creating it for every chunk.

The thread settings on the inference session can also strongly impact performance. ONNX Runtime distinguishes between
intra-op threads, which parallelize work inside a single operator, and inter-op threads, which run independent branches of
the graph in parallel. Increasing the number of threads whilst avoiding oversubscription can pay off during runtime.
The optimal number of threads is again backend-specific; in our experience 2 works well.

```{note}
When benchmarking locally against openEO, set the same ONNX Runtime thread limits you use in the UDF. In our experience
`intra_op_num_threads=2` and `inter_op_num_threads=2` is a good starting point. Comparing a local run with default
all-core settings to a 2-thread UDF run will give misleading timings.
```

```python
import functools

import onnxruntime as ort


@functools.lru_cache(maxsize=1)
def get_session(model_path, num_threads=2):
    options = ort.SessionOptions()
    options.intra_op_num_threads = num_threads
    options.inter_op_num_threads = num_threads
    options.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
    options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
    return ort.InferenceSession(model_path, options, providers=["CPUExecutionProvider"])
```

More threads is not automatically faster, and this is one of the more counter-intuitive things to tune. ONNX Runtime, BLAS,
and PyTorch each try to parallelize across all available cores by default, and when several of these libraries are active
in the same process, or when several worker processes share a node, their thread pools end up competing for the same
physical cores. The practical fix is to tune thread limits together with the executor configuration rather than in
isolation, generally keeping `intra_op_num_threads` close to the number of cores actually allocated to each executor.

## 8. Validation pipelines in openEO

Validation is only informative if it follows the same path as production. It is tempting to validate a model against a
convenient local dataset and call it done, but the goal here is to catch exactly the kinds of drift described above, so the
validation path has to go through the same UDF, the same context object, and ultimately the same backend that will run in
production. A validation that uses a different data path is not really validating the production system; it is validating a
related but distinct system.

A three-stage validation loop tends to catch problems early and cheaply, before they become expensive to debug on a large
job. The first stage checks tensor parity between the original model and the exported model (Example 1 below). The second
stage runs the UDF locally against a representative NetCDF tile,
which is fast to iterate on and catches most shape, dtype, and band-order mistakes. The third stage runs the same UDF
through the actual backend on a tiny, held-out area of interest, which is the only stage that can catch backend-specific
surprises. Each stage is cheap relative to the next, so a problem caught early saves both time and compute.

```{image} /_static/images/geospatial-ai-validation-loop.svg
:alt: Three-stage validation loop: model parity check, local UDF test on a representative tile, then a backend smoke run on a tiny held-out area; failures loop back to an earlier stage.
:width: 100%
:align: center
```

Example 1: source-vs-ONNX parity check.

```python
import numpy as np
import onnxruntime as ort
import torch

# `wrapped` is the model from the export step in 4.1.3, `data` a representative input tensor
with torch.inference_mode():
    source = wrapped(data).detach().cpu().numpy()

session = ort.InferenceSession("encoder.onnx", providers=["CPUExecutionProvider"])
exported = session.run(None, {"data": data.numpy()})[0]

assert source.shape == exported.shape
assert np.allclose(source, exported, rtol=1e-4, atol=1e-5)
```

Example 2: local UDF test on an openEO-generated cube.

```python
import xarray as xr
from udf import apply_datacube

inputs = xr.open_dataset("results/representative_input.nc")
inputs = inputs.drop_vars("crs", errors="ignore")
cube = inputs.to_array(dim="bands").astype("float32").transpose("bands", "t", "y", "x")
tile = cube.isel(y=slice(0, 1024), x=slice(0, 1024))

result = apply_datacube(tile, context={})
```

Example 3: tiny CDSE smoke run before scaling.

Start with a very small area (around 1x1 km), verify the output, and then scale up in small steps.

During this phase, add enough logging inside the UDF. You can retrieve these logs in the openEO
Web Editor (https://editor.openeo.org/) and quickly see what worked and what failed. Logging is
especially valuable here because the UDF runs remotely and you cannot attach a debugger;
the logs are your only window into what the code actually did.

Once the model runs reliably, expand the extent gradually. At some point you will likely hit OOM issues. When that happens,
increasing Python memory or executor memory overhead is usually the first thing to try. If that is
not enough, reach out through the Copernicus Data Space Ecosystem forum
(https://forum.dataspace.copernicus.eu/) for upscaling support, or get in touch with ESA APEx,
which provides algorithm support services (https://apex.esa.int/algorithm-support).

See [4.2](#42-deploying-via-user-defined-functions) for how UDF runtimes, inline dependencies, and dependency
archives work.

### Accuracy assessment and continuous benchmarking

The three stages above check that the production path computes what you intended; they do not tell you how accurate the
model is. For that, run the openEO inference graph over the held-out test samples of the spatially independent split (see
[Spatially independent data splits](#spatially-independent-data-splits)) and compute metrics against the reference labels.
Because sample IDs travel with the extracted values, predictions can be joined to labels sample by sample.

Once a model is operational, turn this into a benchmark that runs repeatedly, so that regressions in accuracy or cost are
caught early:

- Schedule benchmarks periodically, and additionally when the algorithm code, the model, or the platform changes. Unstable
  workflows deserve a higher frequency.
- If benchmarks fail consistently, stop scheduling new runs until the cause is fixed, since every run costs compute.
- Only report differences in accuracy or cost that are statistically significant, and group similar regressions across
  benchmarks, to keep the noise down.
- Use the same benchmark to compare model versions, or alternative algorithms that produce the same output.

## 9. Upscaling and performance tuning

### 9.1 Incremental scaling strategy

Scaling up is best done in phases rather than in one jump from a tiny test to the full operational extent. Start with a
small area of interest and a short time period. After validating the workflow, increase the area and time range in measured
steps, changing the spatial and temporal extents separately while keeping the process graph and job options fixed. Check
coverage, output quality, and runtime after each step. Tune job options separately as described in
[9.2](#92-job-option-tuning).

In practice, this incremental approach pairs well with the
{py:class}`~openeo.extra.job_management.MultiBackendJobManager` (see {ref}`job-manager`), which is built
specifically for running and tracking large numbers of batch jobs rather than babysitting them one at a time. Instead of
manually looping over areas or job options and keeping track of what succeeded, you describe the work as a small table, one row per job,
and let the manager submit, poll, track, and (optionally) download results for you. More orchestration examples are in
[70 · Platform and large scale](https://github.com/Open-EO/openeo-community-examples/tree/main/python/70_platform-and-large-scale).
For an area-scaling sweep, vary only the spatial extent while keeping the process graph and job options fixed:

```python
import pandas as pd
from openeo.extra.job_management import MultiBackendJobManager, create_job_db


# One row per job, each with its own AOI (here: growing squares in UTM zone 31N, EPSG:32631).
scales = pd.DataFrame(
    [
        {"title": "scale_1km", "west": 640000, "south": 5650000, "east": 641000, "north": 5651000},
        {"title": "scale_2km", "west": 640000, "south": 5650000, "east": 642000, "north": 5652000},
        {"title": "scale_5km", "west": 640000, "south": 5650000, "east": 645000, "north": 5655000},
        {"title": "scale_10km", "west": 640000, "south": 5650000, "east": 650000, "north": 5660000},
    ]
)

job_db = create_job_db("scaling_jobs.csv", df=scales)


def start_job(row, connection, **kwargs):
    # Replace this with your actual inference graph, so the sweep measures inference rather than raw data loading.
    cube = connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent={
            "west": row["west"],
            "south": row["south"],
            "east": row["east"],
            "north": row["north"],
            "crs": "EPSG:32631",
        },
        temporal_extent=["2024-04-01", "2024-09-30"],
        bands=["B02", "B03", "B04", "B08"],
    )
    # Return a created (not started) job: the manager starts and tracks it.
    return cube.create_job(
        out_format="GTiff",
        title=row["title"],
    )


manager = MultiBackendJobManager()
manager.add_backend("cdse", connection=connection, parallel_jobs=4)
manager.run_jobs(job_db=job_db, start_job=start_job)
```

Because the job database is persisted to disk (as CSV or Parquet), the whole run can be interrupted and resumed later with
{py:func}`~openeo.extra.job_management.get_job_db` without losing track of which tiles already finished, which is exactly the kind of bookkeeping that becomes
tedious to do by hand once you are past a handful of jobs.

If other tools, such as a dashboard, need to read the job state, the job database can also live in a STAC API with
{py:class}`~openeo.extra.job_management.stac_job_db.STACAPIJobDatabase`. This is still experimental, so pin your client
version when relying on it. When results are written to object storage with `export_workspace` (see
[Delivering patches to storage](#delivering-patches-to-storage)), create the manager with `download_results=False` (see {ref}`job-manager-download-results`), so results are not downloaded a second time.

### 9.2 Job option tuning

For job-option tuning, keep the AOI, time
range, bands, and process graph fixed; change one option at a time and compare runtime, resource use, and output quality.
Backend behavior can affect these results, so validate any improvement with a repeatable run.

```{note}
**CDSE/Geotrellis-specific:** two logged metrics are especially useful when tuning executor options:

- **CPU utilization rate** helps guide `max-executors` tuning; compare it across repeatable runs rather than assuming that more executors always help.
- **Peak executor memory** helps size `executor` and `executor-memoryOverhead`; leave headroom for workload variation.

These metrics and their interpretation are backend-specific, not universal. Validate any adjustment with a repeatable small
run.
```

### 9.3 Cost and credit control

Cost control should start from a representative pilot. When using the {py:class}`~openeo.extra.job_management.MultiBackendJobManager` for the upscaling/tuning exercise, pass
`cancel_running_job_after` (in seconds, see {ref}`job-manager-cancel-running-job`) to cancel jobs that exceed a runtime budget and limit unnecessary credit use. Set
the limit from pilot runtimes, with enough margin for normal variation. For the area sweep in
[9.1](#91-incremental-scaling-strategy), ensure the limit accommodates the largest planned extent; otherwise, omit it for
that sweep.