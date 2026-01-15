# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Added

- Official support for Python 3.14 (include Python 3.14 in unit test matrix on GitHub Actions) ([#801](https://github.com/Open-EO/openeo-python-client/issues/801))

### Changed

### Removed

### Fixed

- Guard STAC metadata parsing against invalid "item_assets" usage ([#853](https://github.com/Open-EO/openeo-python-client/issues/853))


## [0.47.0] - 2025-12-17

### Added

- `MultiBackendJobManager`: add `download_results` option to enable/disable the automated download of job results once completed by the job manager ([#744](https://github.com/Open-EO/openeo-python-client/issues/744))
- Support UDF based spatial and temporal extents in `load_collection`, `load_stac` and `filter_temporal` ([#831](https://github.com/Open-EO/openeo-python-client/pull/831))
- `MultiBackendJobManager`: keep number of "queued" jobs below 10 for better CDSE compatibility ([#839](https://github.com/Open-EO/openeo-python-client/pull/839), eu-cdse/openeo-cdse-infra#859)

### Changed

- Internal reorganization of `openeo.extra.job_management` submodule to ease future development ([#741](https://github.com/Open-EO/openeo-python-client/issues/741))
- `openeo.Connection`: add some more HTTP error codes to the default retry list: `502 Bad Gateway`, `503 Service Unavailable` and `504 Gateway Timeout` ([#835](https://github.com/Open-EO/openeo-python-client/issues/835))

### Removed

- Remove `Connection.load_disk_collection` (wrapper for non-standard `load_disk_data` process), deprecated since version 0.25.0 (related to [Open-EO/openeo-geopyspark-driver#1457](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1457))


## [0.46.0] - 2025-10-31

### Changed

- Move `ProcessBasedJobCreator` to own submodule `openeo.extra.job_management.process_based` ([#741](https://github.com/Open-EO/openeo-python-client/issues/741))

### Removed

- Remove unused/outdated `XarrayDataCube.plot()` and its related matplotlib dependency ([#472](https://github.com/Open-EO/openeo-python-client/issues/472))

### Fixed

- `DataCube.sar_backscatter()`: add corresponding band names to metadata when enabling "mask", "contributing_area", "local_incidence_angle" or "ellipsoid_incidence_angle" ([#804](https://github.com/Open-EO/openeo-python-client/issues/804))
- Proactively refresh access/bearer token in `MultiBackendJobManager` before launching a job start thread ([#817](https://github.com/Open-EO/openeo-python-client/issues/817))
- `Connection.list_services()`: Fix list access error for federation extension


## [0.45.0] - 2025-09-17

### Added

- Initial/experimental implementation of artifact upload helper ([Open-EO/openeo-api#566](https://github.com/Open-EO/openeo-api/issues/566))

### Changed

- `MultiBackendJobManager`: starting of jobs (which can take long in some situations) is now done in side-threads to avoid blocking of the main job management thread, improving its responsiveness and allowing better overall throughput. To make this possible, a new method `get_by_indices()` was added to the `JobDatabaseInterface` API. Make sure to implement this method if you have a custom `JobDatabaseInterface` implementation that does not provide this yet. ([#719](https://github.com/Open-EO/openeo-python-client/issues/719))


## [0.44.0] - 2025-08-20

### Added

- Official support for Python 3.13 (include Python 3.13 in unit test matrix on GitHub Actions) ([#653](https://github.com/Open-EO/openeo-python-client/issues/653))

### Fixed

- `STACAPIJobDatabase.item_from()` use "datetime" from series instead of always taking "now" ([#797](https://github.com/Open-EO/openeo-python-client/issues/797))
- Fix Python 3.13 compatibility issue in `openeo.extra.spectral_indices` ([#799](https://github.com/Open-EO/openeo-python-client/issues/799))


## [0.43.0] - 2025-07-02

### Added

- More extensive band detection for `load_stac` use cases, including the common `bands` metadata introduced with STAC 1.1 ([#699](https://github.com/Open-EO/openeo-python-client/issues/699), [#692](https://github.com/Open-EO/openeo-python-client/issues/692), [#586](https://github.com/Open-EO/openeo-python-client/issues/586)).
- Improved support for Federation Extension in Jupyter notebook context ([#668](https://github.com/Open-EO/openeo-python-client/issues/668))

### Changed

- `openeo.UDF()`: automatically un-indent given UDF code ([#782](https://github.com/Open-EO/openeo-python-client/issues/782))

### Fixed

- Fix compatibility with PySTAC 1.12 ([#715](https://github.com/Open-EO/openeo-python-client/issues/715))


## [0.42.1] - 2025-06-06

### Changed

- Relax `urllib3` dependency constraint below 2.0.0 to unblock dependency resolution issues in some old (Python 3.8) build contexts


## [0.42.0] - 2025-05-28

### Added

- `openeo.testing.io.TestDataLoader`: unit test utility to compactly load (and optionally preprocess) tests data (text/JSON/...)
- `openeo.Connection`: automatically retry API requests on `429 Too Many Requests` HTTP errors, with appropriate delay if possible ([#441](https://github.com/Open-EO/openeo-python-client/issues/441))
- Introduced `pixel_tolerance` argument in `openeo.testing.results` helpers to specify the ignorable fraction of significantly differing pixels. ([#776](https://github.com/Open-EO/openeo-python-client/issues/776))
- `BatchJob.start_and_wait()`: add `require_success` argument (on by default) to control whether an exception should be raised automatically on job failure.

### Changed

- `DataCube.apply_dimension()`: not explicitly specifying the `dimension` argument is deprecated and will trigger warnings ([#774](https://github.com/Open-EO/openeo-python-client/issues/774))
- `BatchJob.start_and_wait()`: all arguments must be specified as keyword arguments to eliminate the risk of positional mix-ups between all its heterogeneous arguments and flags.


## [0.41.0] - 2025-05-14

### Added

- Support `collection_property` based property filtering in `load_stac` ([#246](https://github.com/Open-EO/openeo-python-client/issues/246))
- Add `validate()` method to `SaveResult`, `VectorCube`, `MlModel` and `StacResource` classes ([#766](https://github.com/Open-EO/openeo-python-client/issues/766))
- Added more robust ranged download for large job result files (if supported by the server) ([#747](https://github.com/Open-EO/openeo-python-client/issues/747))

### Changed

- Eliminate deprecated `utcnow` usage patterns. Introduce `Rfc3339.now_utc()` method (as replacement for deprecated `utcnow()` method) to simplify finding deprecated `utcnow` usage in user code. ([#760](https://github.com/Open-EO/openeo-python-client/issues/760))
- `Connection.list_jobs()`: change default `limit` to 100 (instead of fake "unlimited" which was arbitrarily capped in practice anyway) ([#677](https://github.com/Open-EO/openeo-python-client/issues/677))

### Fixed

- Preserve original non-spatial dimensions in `CubeMetadata.resample_cube_spatial()` ([Open-EO/openeo-python-driver#397](https://github.com/Open-EO/openeo-python-driver/issues/397))


## [0.40.0] - 2025-04-14

### Added

- `sar_backscatter`: try to retrieve coefficient options from backend ([#693](https://github.com/Open-EO/openeo-python-client/issues/693))
- Improve error message when OIDC provider is unavailable ([#751](https://github.com/Open-EO/openeo-python-client/issues/751))
- Added `on_response_headers` argument to `DataCube.download()` and related to handle (e.g. `print`) the response headers ([#560](https://github.com/Open-EO/openeo-python-client/issues/560))

### Changed

- When the bands provided to `Connection.load_stac(..., bands=[...])` do not fully match the bands the client extracted from the STAC metadata, a warning will be triggered, but the provided band names will still be used during the client-side preparation of the process graph. This is a pragmatic approach to bridge the gap between differing interpretations of band detection in STAC. Note that this might produce process graphs that are technically invalid and might not work on other backends or future versions of the backend you currently use. It is recommended to consult with the provider of the STAC metadata and openEO backend on the correct and future-proof band names. ([#752](https://github.com/Open-EO/openeo-python-client/issues/752))

### Fixed

- `STACAPIJobDatabase.get_by_status()` now always returns a `pandas.DataFrame` with an index compatible with `MultiBackendJobManager`. ([#707](https://github.com/Open-EO/openeo-python-client/issues/707))


## [0.39.1] - 2025-02-26

### Fixed

- Fix legacy usage pattern to append `export_workspace` to `save_result` with generic `process()` helper method ([#742](https://github.com/Open-EO/openeo-python-client/issues/742))


## [0.39.0] - 2025-02-25

### Added

- Add support for `export_workspace` process ([#720](https://github.com/Open-EO/openeo-python-client/issues/720))
- Add support for [processing parameter extension](https://github.com/Open-EO/openeo-api/tree/draft/extensions/processing-parameters) (e.g. default job options) in `build_process_dict` ([#731](https://github.com/Open-EO/openeo-python-client/issues/731))

### Changed

- `DataCube.save_result()` (and related methods) now return a `SaveResult`/`StacResource` object instead of another `DataCube` object to be more in line with the official `save_result` specification ([#402](https://github.com/Open-EO/openeo-python-client/issues/402), [#720](https://github.com/Open-EO/openeo-python-client/issues/720))
- `datacube_from_flat_graph` now returns a `SaveResult` instead of a `DataCube` when appropriate ([#402](https://github.com/Open-EO/openeo-python-client/issues/402), [#732](https://github.com/Open-EO/openeo-python-client/issues/732), [#733](https://github.com/Open-EO/openeo-python-client/issues/733))
- Deprecate `BatchJob.run_synchronous` in favor of `BatchJob.start_and_wait` ([#570](https://github.com/Open-EO/openeo-python-client/issues/570)).

### Fixed

- Fix incompatibility problem when combining `load_stac` and `resample_spatial` ([#737](https://github.com/Open-EO/openeo-python-client/issues/737))


## [0.38.0] - 2025-02-12

### Added

- Add initial support for accessing [Federation Extension](https://github.com/Open-EO/openeo-api/tree/master/extensions/federation) related metadata ([#668](https://github.com/Open-EO/openeo-python-client/issues/668))

### Changed

- Improved tracking of metadata changes with `resample_spatial` and `resample_cube_spatial` ([#690](https://github.com/Open-EO/openeo-python-client/issues/690))
- Move `ComparableVersion` to `openeo.utils.version` (related to [#611](https://github.com/Open-EO/openeo-python-client/issues/611))
- Deprecate `openeo.rest.rest_capabilities.RESTCapabilities` and introduce replacement `openeo.rest.capabilities.OpenEoCapabilities` ([#611](https://github.com/Open-EO/openeo-python-client/issues/611), [#610](https://github.com/Open-EO/openeo-python-client/issues/610))
- `MultiBackendJobManager`: start new jobs before downloading the results of finished jobs to use time more efficiently ([#633](https://github.com/Open-EO/openeo-python-client/issues/633))

### Removed

- Remove unnecessary base class `openeo.capabilities.Capabilities` [#611](https://github.com/Open-EO/openeo-python-client/issues/611)

### Fixed

- `CsvJobDatabase`: workaround GeoPandas issue (on Python>3.9) when there is a column named "crs" ([#714](https://github.com/Open-EO/openeo-python-client/issues/714))


## [0.37.0] - 2025-01-21 - "SAP10" release

### Added

- Added `show_error_logs` argument to `cube.execute_batch()`/`job.start_and_wait()`/... to toggle the automatic printing of error logs on failure ([#505](https://github.com/Open-EO/openeo-python-client/issues/505))
- Added `Connection.web_editor()` to build link to the openEO backend in the openEO Web Editor
- Add support for `log_level` in `create_job()` and `execute_job()` ([#704](https://github.com/Open-EO/openeo-python-client/issues/704))
- Add initial support for "geometry" dimension type in `CubeMetadata` ([#705](https://github.com/Open-EO/openeo-python-client/issues/705))
- Add support for parameterized `bands` argument in `load_stac()`
- Argument `spatial_extent` in `load_collection()`/`load_stac()`: add support for Shapely objects, loading GeoJSON from a local path and loading geometry from GeoJSON/GeoParquet URL. ([#678](https://github.com/Open-EO/openeo-python-client/issues/678))

### Changed

- Raise exception when providing empty bands array to `load_collection`/`load_stac` ([#424](https://github.com/Open-EO/openeo-python-client/issues/424), [Open-EO/openeo-processes#372](https://github.com/Open-EO/openeo-processes/issues/372))
- Start showing deprecation warnings on usage of GeoJSON "GeometryCollection" (in `filter_spatial`, `aggregate_spatial`, `chunk_polygon`, `mask_polygon`). Use a GeoJSON FeatureCollection instead. ([#706](https://github.com/Open-EO/openeo-python-client/issues/706), [Open-EO/openeo-processes#389](https://github.com/Open-EO/openeo-processes/issues/389))
- The `context` parameter is now used in `execute_local_udf` ([#556](https://github.com/Open-EO/openeo-python-client/issues/556)

### Fixed

- Clear capabilities cache on login ([#254](https://github.com/Open-EO/openeo-python-client/issues/254))


## [0.36.0] - 2024-12-10

### Added

- Automatically use `load_url` when providing a URL as geometries to `DataCube.aggregate_spatial()`, `DataCube.mask_polygon()`, etc. ([#104](https://github.com/Open-EO/openeo-python-client/issues/104), [#457](https://github.com/Open-EO/openeo-python-client/issues/457))
- Allow specifying `limit` when listing batch jobs with `Connection.list_jobs()` ([#677](https://github.com/Open-EO/openeo-python-client/issues/677))
- Add `additional` and `job_options` arguments to `Connection.download()`, `Datacube.download()` and related ([#681](https://github.com/Open-EO/openeo-python-client/issues/681))

### Changed

- `MultiBackendJobManager`: costs has been added as a column in tracking databases ([[#588](https://github.com/Open-EO/openeo-python-client/issues/588)])
- When passing a path/string as `geometry` to `DataCube.aggregate_spatial()`, `DataCube.mask_polygon()`, etc.:
  this is not translated automatically anymore to deprecated, non-standard `read_vector` usage.
  Instead, if it is a local GeoJSON file, the GeoJSON data will be loaded directly client-side.
  ([#104](https://github.com/Open-EO/openeo-python-client/issues/104), [#457](https://github.com/Open-EO/openeo-python-client/issues/457))
- Move `read()` method from general `JobDatabaseInterface` to more specific `FullDataFrameJobDatabase` ([#680](https://github.com/Open-EO/openeo-python-client/issues/680))
- Align `additional` and `job_options` arguments in `Connection.create_job()`, `DataCube.create_job()` and related.
  Also, follow official spec more closely. ([#683](https://github.com/Open-EO/openeo-python-client/issues/683), [Open-EO/openeo-api#276](https://github.com/Open-EO/openeo-api/issues/276))

### Fixed

- `load_stac`: use fallback temporal dimension when no "cube:dimensions" in STAC Collection ([#666](https://github.com/Open-EO/openeo-python-client/issues/666))
- Fix usage of `Parameter.spatial_extent()` with `load_collection` and `filter_bbox` ([#676](https://github.com/Open-EO/openeo-python-client/issues/676))


## [0.35.0] - 2024-11-19

### Added

- Added `MultiResult` helper class to build process graphs with multiple result nodes ([#391](https://github.com/Open-EO/openeo-python-client/issues/391))

### Fixed

- `MultiBackendJobManager`: Fix issue with duplicate job starting across multiple backends ([#654](https://github.com/Open-EO/openeo-python-client/pull/654))
- `MultiBackendJobManager`: Fix encoding issue of job metadata in `on_job_done` ([#657](https://github.com/Open-EO/openeo-python-client/issues/657))
- `MultiBackendJobManager`: Avoid `SettingWithCopyWarning` ([#641](https://github.com/Open-EO/openeo-python-client/issues/641))
- Avoid creating empty file if asset download request failed.
- `MultiBackendJobManager`: avoid dtype loading mistakes in `CsvJobDatabase` on empty columns ([#656](https://github.com/Open-EO/openeo-python-client/issues/656))
- `MultiBackendJobManager`: restore logging of job status histogram during `run_jobs` ([#655](https://github.com/Open-EO/openeo-python-client/issues/655))


## [0.34.0] - 2024-10-31

### Removed

- Drop support for Python 3.7 ([#578](https://github.com/Open-EO/openeo-python-client/issues/578))

### Fixed

- Fixed broken support for `title` and `description` job properties in `execute_batch()` ([#652](https://github.com/Open-EO/openeo-python-client/issues/652))


## [0.33.0] - 2024-10-18

### Added

- Added `DataCube.load_stac()` to also support creating a `load_stac` based cube without a connection ([#638](https://github.com/Open-EO/openeo-python-client/issues/638))
- `MultiBackendJobManager`: Added `initialize_from_df(df)` (to `CsvJobDatabase` and `ParquetJobDatabase`) to initialize (and persist) the job database from a given DataFrame.
  Also added `create_job_db()` factory to easily create a job database from a given dataframe and its type guessed from filename extension.
  ([#635](https://github.com/Open-EO/openeo-python-client/issues/635))
- `MultiBackendJobManager.run_jobs()` now returns a dictionary with counters/stats about various events during the full run of the job manager ([#645](https://github.com/Open-EO/openeo-python-client/issues/645))
- Added (experimental) `ProcessBasedJobCreator` to be used as `start_job` callable with `MultiBackendJobManager` to create multiple jobs from a single parameterized process (e.g. a UDP or remote process definition) ([#604](https://github.com/Open-EO/openeo-python-client/issues/604))

### Fixed

- When using `DataCube.load_collection()` without a connection, it is not necessary anymore to also explicitly set `fetch_metadata=False` ([#638](https://github.com/Open-EO/openeo-python-client/issues/638))


## [0.32.0] - 2024-09-27

### Added

- `load_stac`/`metadata_from_stac`: add support for extracting actual temporal dimension metadata ([#567](https://github.com/Open-EO/openeo-python-client/issues/567))
- `MultiBackendJobManager`: add `cancel_running_job_after` option to automatically cancel jobs that are running for too long ([#590](https://github.com/Open-EO/openeo-python-client/issues/590))
- Added `openeo.api.process.Parameter` helper to easily create a "spatial_extent" UDP parameter
- Wrap OIDC token request failure in more descriptive `OidcException` (related to [#624](https://github.com/Open-EO/openeo-python-client/issues/624))
- Added `auto_add_save_result` option (on by default) to disable automatic addition of `save_result` node on `download`/`create_job`/`execute_batch` ([#513](https://github.com/Open-EO/openeo-python-client/issues/513))
- Add support for `apply_vectorcube` UDF signature in `run_udf_code` ([Open-EO/openeo-geopyspark-driver#881](https://github.com/Open-EO/openeo-geopyspark-driver/issues/811))
- `MultiBackendJobManager`: add API to the update loop in a separate thread, allowing controlled interruption.

### Changed

- `MultiBackendJobManager`: changed job metadata storage API, to enable working with large databases
- `DataCube.apply_polygon()`: rename `polygons` argument to `geometries`, but keep support for legacy `polygons` for now ([#592](https://github.com/Open-EO/openeo-python-client/issues/592), [#511](https://github.com/Open-EO/openeo-processes/issues/511))
- Disallow ambiguous single string argument in `DataCube.filter_temporal()` ([#628](https://github.com/Open-EO/openeo-python-client/issues/628))
- Automatic adding of `save_result` from `download()` or `create_job()`:
  inspect whole process graph for pre-existing `save_result` nodes
  (related to [#623](https://github.com/Open-EO/openeo-python-client/issues/623), [#401](https://github.com/Open-EO/openeo-python-client/issues/401), [#583](https://github.com/Open-EO/openeo-python-client/issues/583))
- Disallow ambiguity of combining explicit `save_result` nodes
  and implicit `save_result` addition from `download()`/`create_job()` calls with `format`
  (related to [#623](https://github.com/Open-EO/openeo-python-client/issues/623), [#401](https://github.com/Open-EO/openeo-python-client/issues/401), [#583](https://github.com/Open-EO/openeo-python-client/issues/583))

### Fixed

- `apply_dimension` with a `target_dimension` argument was not correctly adjusting datacube metadata on the client side, causing a mismatch.
- Preserve non-spatial dimension metadata in `aggregate_spatial` ([#612](https://github.com/Open-EO/openeo-python-client/issues/612))


## [0.31.0] - 2024-07-26

### Added

- Add experimental `openeo.testing.results` subpackage with reusable test utilities for comparing batch job results with reference data
- `MultiBackendJobManager`: add initial support for storing job metadata in Parquet file (instead of CSV) ([#571](https://github.com/Open-EO/openeo-python-client/issues/571))
- Add `Connection.authenticate_oidc_access_token()` to set up authorization headers with an access token that is obtained "out-of-band" ([#598](https://github.com/Open-EO/openeo-python-client/issues/598))
- Add `JobDatabaseInterface` to allow custom job metadata storage with `MultiBackendJobManager` ([#571](https://github.com/Open-EO/openeo-python-client/issues/571))


## [0.30.0] - 2024-06-18

### Added

- Add `openeo.udf.run_code.extract_udf_dependencies()` to extract UDF dependency declarations from UDF code
  (related to [Open-EO/openeo-geopyspark-driver#237](https://github.com/Open-EO/openeo-geopyspark-driver/issues/237))
- Document PEP 723 based Python UDF dependency declarations ([Open-EO/openeo-geopyspark-driver#237](https://github.com/Open-EO/openeo-geopyspark-driver/issues/237))
- Added more `openeo.api.process.Parameter` helpers to easily create "bounding_box", "date", "datetime", "geojson" and "temporal_interval" parameters for UDP construction.
- Added convenience method `Connection.load_stac_from_job(job)` to easily load the results of a batch job with the `load_stac` process ([#566](https://github.com/Open-EO/openeo-python-client/issues/566))
- `load_stac`/`metadata_from_stac`: add support for extracting band info from "item_assets" in collection metadata ([#573](https://github.com/Open-EO/openeo-python-client/issues/573))
- Added initial `openeo.testing` submodule for reusable test utilities

### Fixed

- Initial fix for broken `DataCube.reduce_temporal()` after `load_stac` ([#568](https://github.com/Open-EO/openeo-python-client/pull/568))


## [0.29.0] - 2024-05-03

### Added

- Start depending on `pystac`, initially for better `load_stac` support ([#133](https://github.com/Open-EO/openeo-python-client/issues/133), [#527](https://github.com/Open-EO/openeo-python-client/issues/527))

### Changed

- OIDC device code flow: hide progress bar on completed (or timed out) authentication


## [0.28.0] - 2024-03-18

### Added

- Introduced superclass `CubeMetadata` for `CollectionMetadata` for essential metadata handling (just dimensions for now) without collection-specific STAC metadata parsing. ([#464](https://github.com/Open-EO/openeo-python-client/issues/464))
- Added `VectorCube.vector_to_raster()` ([#550](https://github.com/Open-EO/openeo-python-client/issues/550))

### Changed

- Changed default `chunk_size` of various `download` functions from None to 10MB. This improves the handling of large downloads and reduces memory usage. ([#528](https://github.com/Open-EO/openeo-python-client/issues/528))
- `Connection.execute()` and `DataCube.execute()` now have a `auto_decode` argument. If set to True (default) the response will be decoded as a JSON and throw an exception if this fails, if set to False the raw `requests.Response` object will be returned. ([#499](https://github.com/Open-EO/openeo-python-client/issues/499))

### Fixed

- Preserve geo-referenced `x` and `y` coordinates in `execute_local_udf` ([#549](https://github.com/Open-EO/openeo-python-client/issues/549))


## [0.27.0] - 2024-01-12

### Added

- Add `DataCube.filter_labels()`

### Changed

- Update autogenerated functions/methods in `openeo.processes` to definitions from openeo-processes project version 2.0.0-rc1.
  This removes `create_raster_cube`, `fit_class_random_forest`, `fit_regr_random_forest` and `save_ml_model`.
  Although removed from openeo-processes 2.0.0-rc1, support for `load_result`, `predict_random_forest` and `load_ml_model`
  is preserved but deprecated. ([#424](https://github.com/Open-EO/openeo-python-client/issues/424))
- Show more informative error message on `403 Forbidden` errors from CDSE firewall ([#512](https://github.com/Open-EO/openeo-python-client/issues/512))
- Handle API error responses more strict and avoid hiding possibly important information in JSON-formatted but non-compliant error responses.

### Fixed

- Fix band name support in `DataCube.band()` when no metadata is available ([#515](https://github.com/Open-EO/openeo-python-client/issues/515))
- Support optional child callbacks in generated `openeo.processes`, e.g. `merge_cubes` ([#522](https://github.com/Open-EO/openeo-python-client/issues/522))
- Fix broken pre-flight validation in `Connection.save_user_defined_process` ([#526](https://github.com/Open-EO/openeo-python-client/issues/526))


## [0.26.0] - 2023-11-27 - "SRR6" release

### Added

- Support new UDF signature: `def apply_datacube(cube: DataArray, context: dict) -> DataArray`
  ([#310](https://github.com/Open-EO/openeo-python-client/issues/310))
- Add `collection_property()` helper to easily build collection metadata property filters for `Connection.load_collection()`
  ([#331](https://github.com/Open-EO/openeo-python-client/pull/331))
- Add `DataCube.apply_polygon()` (standardized version of experimental `chunk_polygon`) ([#424](https://github.com/Open-EO/openeo-python-client/issues/424))
- Various improvements to band mapping with the Awesome Spectral Indices feature.
  Allow explicitly specifying the satellite platform for band name mapping (e.g. "Sentinel2" or "LANDSAT8") if cube metadata lacks info.
  Follow the official band mapping from Awesome Spectral Indices better.
  Allow manually specifying the desired band mapping.
  ([#485](https://github.com/Open-EO/openeo-python-client/issues/485), [#501](https://github.com/Open-EO/openeo-python-client/issues/501))
- Also attempt to automatically refresh OIDC access token on a `401 TokenInvalid` response (in addition to `403 TokenInvalid`) ([#508](https://github.com/Open-EO/openeo-python-client/issues/508))
- Add `Parameter.object()` factory for `object` type parameters

### Removed

- Remove custom spectral indices "NDGI", "NDMI" and "S2WI" from "extra-indices-dict.json"
  that were shadowing the official definitions from Awesome Spectral Indices ([#501](https://github.com/Open-EO/openeo-python-client/issues/501))

### Fixed

- Initial support for "spectral indices" that use constants defined by Awesome Spectral Indices ([#501](https://github.com/Open-EO/openeo-python-client/issues/501))


## [0.25.0] - 2023-11-02

### Changed

- Introduce `OpenEoApiPlainError` for API error responses that are not well-formed
  for better distinction with properly formed API error responses (`OpenEoApiError`).
  ([#491](https://github.com/Open-EO/openeo-python-client/issues/491)).

### Fixed

- Fix missing `validate` support in `LocalConnection.execute` ([#493](https://github.com/Open-EO/openeo-python-client/pull/493))


## [0.24.0] - 2023-10-27

### Added

- Add `DataCube.reduce_spatial()`
- Added option (enabled by default) to automatically validate a process graph before execution.
  Validation issues just trigger warnings for now. ([#404](https://github.com/Open-EO/openeo-python-client/issues/404))
- Added "Sentinel1" band mapping support to "Awesome Spectral Indices" wrapper ([#484](https://github.com/Open-EO/openeo-python-client/issues/484))
- Run tests in GitHub Actions against Python 3.12 as well

### Changed

- Enforce `XarrayDataCube` dimension order in `execute_local_udf()` to (t, bands, y, x)
  to improve UDF interoperability with existing back-end implementations.


## [0.23.0] - 2023-10-02

### Added

- Support year/month shorthand date notations in temporal extent arguments of `Connection.load_collection`, `DataCube.filter_temporal` and related ([#421](https://github.com/Open-EO/openeo-python-client/issues/421))
- Support parameterized `bands` in `load_collection` ([#471](https://github.com/Open-EO/openeo-python-client/issues/471))
- Allow specifying item schema in `Parameter.array()`
- Support "subtype" and "format" schema options in `Parameter.string()`

### Changed

- Before doing user-defined process (UDP) listing/creation: verify that back-end supports that (through openEO capabilities document) to improve error message.
- Skip metadata-based normalization/validation and stop showing unhelpful warnings/errors
  like "No cube:dimensions metadata" or "Invalid dimension"
  when no metadata is available client-side anyway (e.g. when using `datacube_from_process`, parameterized cube building, ...).
  ([#442](https://github.com/Open-EO/openeo-python-client/issues/442))

### Removed

- Bumped minimal supported Python version to 3.7 ([#460](https://github.com/Open-EO/openeo-python-client/issues/460))

### Fixed

- Support handling of "callback" parameters in `openeo.processes` callables ([#470](https://github.com/Open-EO/openeo-python-client/issues/470))


## [0.22.0] - 2023-08-09

### Added

- Processes that take a CRS as argument now try harder to normalize your input to
  a CRS representation that aligns with the openEO API (using `pyproj` library when available)
  ([#259](https://github.com/Open-EO/openeo-python-client/issues/259))
- Initial `load_geojson` support with `Connection.load_geojson()` ([#424](https://github.com/Open-EO/openeo-python-client/issues/424))
- Initial `load_url` (for vector cubes) support with `Connection.load_url()` ([#424](https://github.com/Open-EO/openeo-python-client/issues/424))
- Add `VectorCube.apply_dimension()` ([Open-EO/openeo-python-driver#197](https://github.com/Open-EO/openeo-python-driver/issues/197))
- Support lambda based property filtering in `Connection.load_stac()` ([#425](https://github.com/Open-EO/openeo-python-client/issues/425))
- `VectorCube`: initial support for `filter_bands`, `filter_bbox`, `filter_labels` and `filter_vector` ([#459](https://github.com/Open-EO/openeo-python-client/issues/459))

### Changed

- `Connection` based requests: always use finite timeouts by default (20 minutes in general, 30 minutes for synchronous execute requests)
  ([#454](https://github.com/Open-EO/openeo-python-client/issues/454))

### Fixed

- Fix: MultibackendJobManager should stop when finished, also when job finishes with error ([#452](https://github.com/Open-EO/openeo-python-client/issues/432))


## [0.21.1] - 2023-07-19

### Fixed

- Fix `spatial_extent`/`temporal_extent` handling in "localprocessing" `load_stac` ([#451](https://github.com/Open-EO/openeo-python-client/pull/451))


## [0.21.0] - 2023-07-19

### Added

- Add support in `VectoCube.download()` and `VectorCube.execute_batch()` to guess output format from extension of a given filename
  ([#401](https://github.com/Open-EO/openeo-python-client/issues/401), [#449](https://github.com/Open-EO/openeo-python-client/issues/449))
- Added `load_stac` for Client Side Processing, based on the [openeo-processes-dask implementation](https://github.com/Open-EO/openeo-processes-dask/pull/127)

### Changed

- Updated docs for Client Side Processing with `load_stac` examples, available at https://open-eo.github.io/openeo-python-client/cookbook/localprocessing.html

### Fixed

- Avoid double `save_result` nodes when combining `VectorCube.save_result()` and `.download()`.
  ([#401](https://github.com/Open-EO/openeo-python-client/issues/401), [#448](https://github.com/Open-EO/openeo-python-client/issues/448))


## [0.20.0] - 2023-06-30

### Added

- Added automatically renewal of access tokens with OIDC client credentials grant (`Connection.authenticate_oidc_client_credentials`)
  ([#436](https://github.com/Open-EO/openeo-python-client/issues/436))

### Changed

- Simplified `BatchJob` methods `start()`, `stop()`, `describe()`, ...
  Legacy aliases `start_job()`, `describe_job()`, ... are still available and don't trigger a deprecation warning for now.
  ([#280](https://github.com/Open-EO/openeo-python-client/issues/280))
- Update `openeo.extra.spectral_indices` to [Awesome Spectral Indices v0.4.0](https://github.com/awesome-spectral-indices/awesome-spectral-indices/releases/tag/0.4.0)


## [0.19.0] - 2023-06-16

### Added

- Generalized support for setting (default) OIDC provider id through env var `OPENEO_AUTH_PROVIDER_ID`
  [#419](https://github.com/Open-EO/openeo-python-client/issues/419)
- Added `OidcDeviceCodePollTimeout`: specific exception for OIDC device code flow poll timeouts
- On-demand preview: Added `DataCube.preview()` to generate a XYZ service with the process graph and display a map widget

### Fixed

- Fix format option conflict between `save_result` and `create_job`
  [#433](https://github.com/Open-EO/openeo-python-client/issues/433)
- Ensure that OIDC device code link opens in a new tab/window [#443](https://github.com/Open-EO/openeo-python-client/issues/443)


## [0.18.0] - 2023-05-31

### Added

- Support OIDC client credentials grant from a generic `connection.authenticate_oidc()` call
  through environment variables
  [#419](https://github.com/Open-EO/openeo-python-client/issues/419)

### Fixed

- Fixed UDP parameter conversion issue in `build_process_dict` when using parameter in `context` of `run_udf`
  [#431](https://github.com/Open-EO/openeo-python-client/issues/431)


## [0.17.0] and [0.17.1] - 2023-05-16

### Added

- `Connection.authenticate_oidc()`: add argument `max_poll_time` to set maximum device code flow poll time
- Show progress bar while waiting for OIDC authentication with device code flow,
  including special mode for in Jupyter notebooks.
  ([#237](https://github.com/Open-EO/openeo-python-client/issues/237))
- Basic support for `load_stac` process with `Connection.load_stac()`
  ([#425](https://github.com/Open-EO/openeo-python-client/issues/425))
- Add `DataCube.aggregate_spatial_window()`

### Fixed

- Include "scope" parameter in OIDC token request with client credentials grant.
- Support fractional seconds in `Rfc3339.parse_datetime`
  ([#418](https://github.com/Open-EO/openeo-python-client/issues/418))

## [0.16.0] - 2023-04-17 - "SRR5" release

### Added

- Full support for user-uploaded files (`/files` endpoints)
  ([#377](https://github.com/Open-EO/openeo-python-client/issues/377))
- Initial, experimental "local processing" feature to use
  openEO Python Client Library functionality on local
  GeoTIFF/NetCDF files and also do the processing locally
  using the `openeo_processes_dask` package
  ([#338](https://github.com/Open-EO/openeo-python-client/pull/338))
- Add `BatchJob.get_results_metadata_url()`.

### Changed

- `Connection.list_files()` returns a list of `UserFile` objects instead of a list of metadata dictionaries.
  Use `UserFile.metadata` to get the original dictionary.
  ([#377](https://github.com/Open-EO/openeo-python-client/issues/377))
- `DataCube.aggregate_spatial()` returns a `VectorCube` now, instead of a `DataCube`
  ([#386](https://github.com/Open-EO/openeo-python-client/issues/386)).
  The (experimental) `fit_class_random_forest()` and `fit_regr_random_forest()` methods
  moved accordingly to the `VectorCube` class.
- Improved documentation on `openeo.processes` and `ProcessBuilder`
  ([#390](https://github.com/Open-EO/openeo-python-client/issues/390)).
- `DataCube.create_job()` and `Connection.create_job()` now require
  keyword arguments for all but the first argument for clarity.
  ([#412](https://github.com/Open-EO/openeo-python-client/issues/412)).
- Pass minimum log level to backend when retrieving batch job and secondary service logs.
  ([Open-EO/openeo-api#485](https://github.com/Open-EO/openeo-api/issues/485),
  [Open-EO/openeo-python-driver#170](https://github.com/Open-EO/openeo-python-driver/issues/170))


### Removed

- Dropped support for pre-1.0.0 versions of the openEO API
  ([#134](https://github.com/Open-EO/openeo-python-client/issues/134)):
  - Remove `ImageCollectionClient` and related helpers
    (now unused leftovers from version 0.4.0 and earlier).
    (Also [#100](https://github.com/Open-EO/openeo-python-client/issues/100))
  - Drop support for pre-1.0.0 job result metadata
  - Require at least version 1.0.0 of the openEO API for a back-end in `Connection`
    and all its methods.

### Fixed

- Reinstated old behavior of authentication related user files (e.g. refresh token store) on Windows: when `PrivateJsonFile` may be readable by others, just log a message instead of raising `PermissionError` ([387](https://github.com/Open-EO/openeo-python-client/issues/387))
- `VectorCube.create_job()` and `MlModel.create_job()` are properly aligned with `DataCube.create_job()`
  regarding setting job title, description, etc.
  ([#412](https://github.com/Open-EO/openeo-python-client/issues/412)).
- More robust handling of billing currency/plans in capabilities
  ([#414](https://github.com/Open-EO/openeo-python-client/issues/414))
- Avoid blindly adding a `save_result` node from `DataCube.execute_batch()` when there is already one
  ([#401](https://github.com/Open-EO/openeo-python-client/issues/401))


## [0.15.0] - 2023-03-03

### Added

- The openeo Python client library can now also be installed with conda (conda-forge channel)
  ([#176](https://github.com/Open-EO/openeo-python-client/issues/176))
- Allow using a custom `requests.Session` in `openeo.rest.auth.oidc` logic

### Changed

- Less verbose log printing on failed batch job [#332](https://github.com/Open-EO/openeo-python-client/issues/332)
- Improve (UTC) timezone handling in `openeo.util.Rfc3339` and add `rfc3339.today()`/`rfc3339.utcnow()`.


## [0.14.1] - 2023-02-06

### Fixed

- Fine-tuned `XarrayDataCube` tests for conda building and packaging ([#176](https://github.com/Open-EO/openeo-python-client/issues/176))


## [0.14.0] - 2023-02-01

### Added

- Jupyter integration: show process graph visualization of `DataCube` objects instead of generic `repr`.  ([#336](https://github.com/Open-EO/openeo-python-client/issues/336))
- Add `Connection.vectorcube_from_paths()` to load a vector cube
  from files (on back-end) or URLs with `load_uploaded_files` process.
- Python 3.10 and 3.11 are now officially supported
  (test run now also for 3.10 and 3.11 in GitHub Actions, [#346](https://github.com/Open-EO/openeo-python-client/issues/346))
- Support for simplified OIDC device code flow, ([#335](https://github.com/Open-EO/openeo-python-client/issues/335))
- Added MultiBackendJobManager, based on implementation from openeo-classification project
  ([#361](https://github.com/Open-EO/openeo-python-client/issues/361))
- Added resilience to MultiBackendJobManager for backend failures ([#365](https://github.com/Open-EO/openeo-python-client/issues/365))

### Changed

- `execute_batch` also skips temporal `502 Bad Gateway errors`. [#352](https://github.com/Open-EO/openeo-python-client/issues/352)

### Fixed

- Fixed/improved math operator/process support for `DataCube`s in "apply" mode (non-"band math"),
  allowing expressions like `10 * cube.log10()` and `~(cube == 0)`
  ([#123](https://github.com/Open-EO/openeo-python-client/issues/123))
- Support `PrivateJsonFile` permissions properly on Windows, using oschmod library.
  ([#198](https://github.com/Open-EO/openeo-python-client/issues/198))
- Fixed some broken unit tests on Windows related to path (separator) handling.
  ([#350](https://github.com/Open-EO/openeo-python-client/issues/350))


## [0.13.0] - 2022-10-10 - "UDF UX" release

### Added

- Add `max_cloud_cover` argument to `load_collection()` to simplify setting maximum cloud cover (property `eo:cloud_cover`) ([#328](https://github.com/Open-EO/openeo-python-client/issues/328))

### Changed
- Improve default dimension metadata of a datacube created with `openeo.rest.datacube.DataCube.load_disk_collection`
- `DataCube.download()`: only automatically add `save_result` node when there is none yet.
- Deprecation warnings: make sure they are shown by default and can be hidden when necessary.
- Rework and improve `openeo.UDF` helper class for UDF usage
  ([#312](https://github.com/Open-EO/openeo-python-client/issues/312)).
    - allow loading directly from local file or URL
    - autodetect `runtime` from file/URL suffix or source code
    - hide implementation details around `data` argument (e.g.`data={"from_parameter": "x"}`)
    - old usage patterns of `openeo.UDF` and `DataCube.apply_dimension()` still work but trigger deprecation warnings
- Show warning when using `load_collection` property filters that are not defined in the collection metadata (summaries).



## [0.12.1] - 2022-09-15

### Changed

- Eliminate dependency on `distutils.version.LooseVersion` which started to trigger deprecation warnings  ([#316](https://github.com/Open-EO/openeo-python-client/issues/316)).

### Removed

- Remove old `Connection.oidc_auth_user_id_token_as_bearer` workaround flag ([#300](https://github.com/Open-EO/openeo-python-client/issues/300))

### Fixed

- Fix refresh token handling in case of OIDC token request with refresh token grant ([#326](https://github.com/Open-EO/openeo-python-client/issues/326))


## [0.12.0] - 2022-09-09

### Added

- Allow passing raw JSON string, JSON file path or URL to `Connection.download()`,
  `Connection.execute()` and `Connection.create_job()`
- Add support for reverse math operators on DataCube in `apply` mode ([#323](https://github.com/Open-EO/openeo-python-client/issues/323))
- Add `DataCube.print_json()` to simplify exporting process graphs in Jupyter or other interactive environments ([#324](https://github.com/Open-EO/openeo-python-client/issues/324))
- Raise `DimensionAlreadyExistsException` when trying to `add_dimension()` a dimension with existing name ([Open-EO/openeo-geopyspark-driver#205](https://github.com/Open-EO/openeo-geopyspark-driver/issues/205))

### Changed

- `DataCube.execute_batch()` now also guesses the output format from the filename,
  and allows using `format` argument next to the current `out_format`
  to align with the `DataCube.download()` method. ([#240](https://github.com/Open-EO/openeo-python-client/issues/240))
- Better client-side handling of merged band name metadata in `DataCube.merge_cubes()`

### Removed

- Remove legacy `DataCube.graph` and `DataCube.flatten()` to prevent usage patterns that cause interoperability issues
  ([#155](https://github.com/Open-EO/openeo-python-client/issues/155), [#209](https://github.com/Open-EO/openeo-python-client/issues/209), [#324](https://github.com/Open-EO/openeo-python-client/issues/324))


## [0.11.0] - 2022-07-02

### Added

- Add support for passing a PGNode/VectorCube as geometry to `aggregate_spatial`, `mask_polygon`, ...
- Add support for second order callbacks e.g. `is_valid` in `count` in `reduce_dimension` ([#317](https://github.com/Open-EO/openeo-python-client/issues/317))

### Changed

- Rename `RESTJob` class name to less cryptic and more user-friendly `BatchJob`.
  Original `RESTJob` is still available as deprecated alias.
  ([#280](https://github.com/Open-EO/openeo-python-client/issues/280))
- Dropped default reducer ("max") from `DataCube.reduce_temporal_simple()`
- Various documentation improvements:
    - general styling, landing page and structure tweaks ([#285](https://github.com/Open-EO/openeo-python-client/issues/285))
    - batch job docs ([#286](https://github.com/Open-EO/openeo-python-client/issues/286))
    - getting started docs ([#308](https://github.com/Open-EO/openeo-python-client/issues/308))
    - part of UDF docs ([#309](https://github.com/Open-EO/openeo-python-client/issues/309))
    - added process-to-method mapping docs
- Drop hardcoded `h5netcdf` engine from `XarrayIO.from_netcdf_file()`
  and `XarrayIO.to_netcdf_file()` ([#314](https://github.com/Open-EO/openeo-python-client/issues/314))
- Changed argument name of `Connection.describe_collection()` from `name` to `collection_id`
  to be more in line with other methods/functions.

### Fixed

- Fix `context`/`condition` confusion bug with `count` callback in `DataCube.reduce_dimension()` ([#317](https://github.com/Open-EO/openeo-python-client/issues/317))



## [0.10.1] - 2022-05-18 - "LPS22" release

### Added

- Add `context` parameter to `DataCube.aggregate_spatial()`, `DataCube.apply_dimension()`,
  `DataCube.apply_neighborhood()`, `DataCube.apply()`, `DataCube.merge_cubes()`.
  ([#291](https://github.com/Open-EO/openeo-python-client/issues/291))
- Add `DataCube.fit_regr_random_forest()` ([#293](https://github.com/Open-EO/openeo-python-client/issues/293))
- Add `PGNode.update_arguments()`, which combined with `DataCube.result_node()` allows to do advanced process graph argument tweaking/updating without using `._pg` hacks.
- `JobResults.download_files()`: also download (by default) the job result metadata as STAC JSON file ([#184](https://github.com/Open-EO/openeo-python-client/issues/184))
- OIDC handling in `Connection`: try to automatically refresh access token when expired ([#298](https://github.com/Open-EO/openeo-python-client/issues/298))
- `Connection.create_job` raises exception if response does not contain a valid job_id
- Add `openeo.udf.debug.inspect` for using the openEO `inspect` process in a UDF ([#302](https://github.com/Open-EO/openeo-python-client/issues/302))
- Add `openeo.util.to_bbox_dict()` to simplify building a openEO style bbox dictionary, e.g. from a list or shapely geometry ([#304](https://github.com/Open-EO/openeo-python-client/issues/304))

### Removed

- Removed deprecated (and non-functional) `zonal_statistics` method from old `ImageCollectionClient` API. ([#144](https://github.com/Open-EO/openeo-python-client/issues/144))


## [0.10.0] - 2022-04-08 - "SRR3" release

### Added

- Add support for comparison operators (`<`, `>`, `<=` and `>=`) in callback process building
- Added `Connection.describe_process()` to retrieve and show a single process
- Added `DataCube.flatten_dimensions()` and `DataCube.unflatten_dimension`
  ([Open-EO/openeo-processes#308](https://github.com/Open-EO/openeo-processes/issues/308), [Open-EO/openeo-processes#316](https://github.com/Open-EO/openeo-processes/pull/316))
- Added `VectorCube.run_udf` (to avoid non-standard `process_with_node(UDF(...))` usage)
- Added `DataCube.fit_class_random_forest()` and `Connection.load_ml_model()` to train and load Machine Learning models
  ([#279](https://github.com/Open-EO/openeo-python-client/issues/279))
- Added `DataCube.predict_random_forest()` to easily use `reduce_dimension` with a `predict_random_forest` reducer
  using a `MlModel` (trained with `fit_class_random_forest`)
  ([#279](https://github.com/Open-EO/openeo-python-client/issues/279))
- Added `DataCube.resample_cube_temporal` ([#284](https://github.com/Open-EO/openeo-python-client/issues/284))
- Add `target_dimension` argument to `DataCube.aggregate_spatial` ([#288](https://github.com/Open-EO/openeo-python-client/issues/288))
- Add basic configuration file system to define a default back-end URL and enable auto-authentication ([#264](https://github.com/Open-EO/openeo-python-client/issues/264), [#187](https://github.com/Open-EO/openeo-python-client/issues/187))
- Add `context` argument to `DataCube.chunk_polygon()`
- Add `Connection.version_info()` to list version information about the client, the API and the back-end

### Changed

- Include openEO API error id automatically in exception message to simplify user support and post-mortem analysis.
- Use `Connection.default_timeout` (when set) also on version discovery request
- Drop `ImageCollection` from `DataCube`'s class hierarchy.
  This practically removes very old (pre-0.4.0) methods like `date_range_filter` and `bbox_filter` from `DataCube`.
  ([#100](https://github.com/Open-EO/openeo-python-client/issues/100), [#278](https://github.com/Open-EO/openeo-python-client/issues/278))
- Deprecate `DataCube.send_job` in favor of `DataCube.create_job` for better consistency (internally and with other libraries) ([#276](https://github.com/Open-EO/openeo-python-client/issues/276))
- Update (autogenerated) `openeo.processes` module to 1.2.0 release (2021-12-13) of openeo-processes
- Update (autogenerated) `openeo.processes` module to draft version of 2022-03-16 (e4df8648) of openeo-processes
- Update `openeo.extra.spectral_indices` to a post-0.0.6 version of [Awesome Spectral Indices](https://awesome-ee-spectral-indices.readthedocs.io/en/latest/)

### Removed

- Removed deprecated `zonal_statistics` method from `DataCube`. ([#144](https://github.com/Open-EO/openeo-python-client/issues/144))
- Deprecate old-style `DataCube.polygonal_mean_timeseries()`, `DataCube.polygonal_histogram_timeseries()`,
  `DataCube.polygonal_median_timeseries()` and `DataCube.polygonal_standarddeviation_timeseries()`

### Fixed

- Support `rename_labels` on temporal dimension ([#274](https://github.com/Open-EO/openeo-python-client/issues/274))
- Basic support for mixing `DataCube` and `ProcessBuilder` objects/processing ([#275](https://github.com/Open-EO/openeo-python-client/issues/275))


## [0.9.2] - 2022-01-14

### Added

- Add experimental support for `chunk_polygon` process ([Open-EO/openeo-processes#287](https://github.com/Open-EO/openeo-processes/issues/287))
- Add support for `spatial_extent`, `temporal_extent` and `bands` to `Connection.load_result()`
- Setting the environment variable `OPENEO_BASEMAP_URL` allows to set a new templated URL to a XYZ basemap for the Vue Components library,  `OPENEO_BASEMAP_ATTRIBUTION` allows to set the attribution for the basemap ([#260](https://github.com/Open-EO/openeo-python-client/issues/260))
- Initial support for experimental "federation:missing" flag on partial openEO Platform user job listings ([Open-EO/openeo-api#419](https://github.com/Open-EO/openeo-api/pull/419))
- Best effort detection of mistakenly using Python builtin `sum` or `all` functions in callbacks ([Forum #113](https://discuss.eodc.eu/t/reducing-masks-in-openeo/113))
- Automatically print batch job logs when job doesn't finish successfully (using `execute_batch/run_synchronous/start_and_wait`).


## [0.9.1] - 2021-11-16

### Added

- Add `options` argument to `DataCube.atmospheric_correction` ([Open-EO/openeo-python-driver#91](https://github.com/Open-EO/openeo-python-driver/issues/91))
- Add `atmospheric_correction_options` and `cloud_detection_options` arguments to `DataCube.ard_surface_reflectance` ([Open-EO/openeo-python-driver#91](https://github.com/Open-EO/openeo-python-driver/issues/91))
- UDP storing: add support for "returns", "categories", "examples" and "links" properties ([#242](https://github.com/Open-EO/openeo-python-client/issues/242))
- Add `openeo.extra.spectral_indices`: experimental API to easily compute spectral indices (vegetation, water, urban, ...)
  on a `DataCube`, using the index definitions from [Awesome Spectral Indices](https://awesome-ee-spectral-indices.readthedocs.io/en/latest/)


### Changed

- Batch job status poll loop: ignore (temporary) "service unavailable" errors ([Open-EO/openeo-python-driver#96](https://github.com/Open-EO/openeo-python-driver/issues/96))
- Batch job status poll loop: fail when there are too many soft errors (temporary connection/availability issues)


### Fixed

- Fix `DataCube.ard_surface_reflectance()` to use process `ard_surface_reflectance` instead of `atmospheric_correction`


## [0.9.0] - 2021-10-11

### Added

- Add command line tool `openeo-auth token-clear` to remove OIDC refresh token cache
- Add support for OIDC device authorization grant without PKCE nor client secret,
  ([#225](https://github.com/Open-EO/openeo-python-client/issues/225), [openeo-api#410](https://github.com/Open-EO/openeo-api/issues/410))
- Add `DataCube.dimension_labels()` (EP-4008)
- Add `Connection.load_result()` (EP-4008)
- Add proper support for child callbacks in `fit_curve` and `predict_curve` ([#229](https://github.com/Open-EO/openeo-python-client/issues/229))
- `ProcessBuilder`: Add support for `array_element(data, n)` through `data[n]` syntax ([#228](https://github.com/Open-EO/openeo-python-client/issues/228))
- `ProcessBuilder`: Add support for `eq` and `neq` through `==` and `!=` operators (EP-4011)
- Add `DataCube.validate()` for process graph validation (EP-4012 related)
- Add `Connection.as_curl()` for generating curl command to evaluate a process graph or `DataCube` from the command line
- Add support in `DataCube.download()` to guess output format from extension of a given filename


### Changed

- Improve default handling of `crs` (and `base`/`height`) in `filter_bbox`: avoid explicitly sending `null` unnecessarily
  ([#233](https://github.com/Open-EO/openeo-python-client/pull/233)).
- Update documentation/examples/tests: EPSG CRS in `filter_bbox` should be integer code, not string
  ([#233](https://github.com/Open-EO/openeo-python-client/pull/233)).
- Raise `ProcessGraphVisitException` from `ProcessGraphVisitor.resolve_from_node()` (instead of generic `ValueError`)
- `DataCube.linear_scale_range` is now a shortcut for `DataCube.apply(lambda  x:x.x.linear_scale_range( input_min, input_max, output_min, output_max))`.
   Instead of creating an invalid process graph that tries to invoke linear_scale_range on a datacube directly.
- Nicer error message when back-end does not support basic auth ([#247](https://github.com/Open-EO/openeo-python-client/issues/247))


### Removed

- Remove unused and outdated (0.4-style) `File`/`RESTFile` classes ([#115](https://github.com/Open-EO/openeo-python-client/issues/115))
- Deprecate usage of `DataCube.graph` property ([#209](https://github.com/Open-EO/openeo-python-client/issues/209))


## [0.8.2] - 2021-08-24

Minor release to address version packaging issue.

## [0.8.1] - 2021-08-24

### Added

- Support nested callbacks inside array arguments, for instance in `array_modify`, `array_create`
- Support `array_concat`
- add `ProcessGraphUnflattener` and `PGNodeGraphUnflattener` to unflatten a flat dict representation of a process
  graph to a `PGNode` graph (EP-3609)
- Add `Connection.datacube_from_flat_graph` and `Connection.datacube_from_json` to construct a `DataCube`
  from flat process graph representation (e.g. JSON file or JSON URL) (EP-3609)
- Add documentation about UDP unflattening and sharing (EP-3609)
- Add `fit_curve` and `predict_curve`, two methods used in change detection

### Changed

- Update `processes.py` based on 1.1.0 release op openeo-processes project
- `processes.py`: include all processes from "proposals" folder of openeo-processes project
- Jupyter integration: Visual rendering for process graphs shown instead of a plain JSON representation.
- Migrate from Travis CI to GitHub Actions for documentation building and unit tests ([#178](https://github.com/Open-EO/openeo-python-client/issues/178), EP-3645)

### Removed

- Removed unit test runs for Python 3.5 ([#210](https://github.com/Open-EO/openeo-python-client/issues/210))


## [0.8.0] - 2021-06-25

### Added

- Allow, but raise warning when specifying a CRS for the geometry passed to `aggregate_spatial` and `mask_polygon`,
  which is non-standard/experimental feature, only supported by specific back-ends
  ([#204](https://github.com/Open-EO/openeo-python-client/issues/204))
- Add `optional` argument to `Parameter` and fix re-encoding parameters with default value. (EP-3846)
- Add support to test strict equality with `ComparableVersion`
- Jupyter integration: add rich HTML rendering for more backend metadata (Job, Job Estimate, Logs, Services, User-Defined Processes)
- Add support for [filter_spatial](https://processes.openeo.org/#filter_spatial)
- Add support for [aggregate_temporal_period](https://processes.openeo.org/#aggregate_temporal_period)
- Added class `Service` for secondary web-services
- Added a method `service` to `Connection`
- Add `Rfc3339.parse_date` and `Rfc3339.parse_date_or_datetime`

### Changed

- Disallow redirects on POST/DELETE/... requests and require status code 200 on `POST /result` requests.
  This improves error information where `POST /result` would involve a redirect. (EP-3889)
- Class `JobLogEntry` got replaced with a more complete and re-usable `LogEntry` dict
- The following methods return a `Service` class instead of a dict: `tiled_viewing_service` in `ImageCollection`, `ImageCollectionClient` and `DataCube`, `create_service` in `Connection`

### Deprecated

- The method `remove_service` in `Connection` has been deprecated in favor of `delete_service` in the `Service` class


## [0.7.0] - 2021-04-21

### Added

- Add dependency on `xarray` package ([#159](https://github.com/Open-EO/openeo-python-client/issues/159), [#190](https://github.com/Open-EO/openeo-python-client/pull/190), EP-3578)
- Add support for default OIDC clients advertised by backend ([#192](https://github.com/Open-EO/openeo-python-client/issues/192), [Open-EO/openeo-api#366](https://github.com/Open-EO/openeo-api/pull/366))
- Add support for default OIDC provider (based on provider order advertised by backend) ([Open-EO/openeo-api#373](https://github.com/Open-EO/openeo-api/pull/373))

### Changed

- Eliminate development/optional dependency on `openeo_udf` project
  ([#159](https://github.com/Open-EO/openeo-python-client/issues/159), [#190](https://github.com/Open-EO/openeo-python-client/pull/190), EP-3578).
  Now the openEO client library itself contains the necessary classes and implementation to run UDF code locally.

### Fixed

- `Connection`: don't send default auth headers to non-backend domains ([#201](https://github.com/Open-EO/openeo-python-client/issues/201))


## [0.6.1] - 2021-03-29

### Changed

- Improve OpenID Connect usability on Windows: don't raise exception on file permissions
  that can not be changed (by `os.chmod` on Windows) ([#198](https://github.com/Open-EO/openeo-python-client/issues/198))


## [0.6.0] - 2021-03-26

### Added

- Add initial/experimental support for OIDC device code flow with PKCE (alternative for client secret) ([#191](https://github.com/Open-EO/openeo-python-client/issues/191) / EP-3700)
- When creating a connection: use "https://" by default when no protocol is specified
- `DataCube.mask_polygon`: support `Parameter` argument for `mask`
- Add initial/experimental support for default OIDC client ([#192](https://github.com/Open-EO/openeo-python-client/issues/192), [Open-EO/openeo-api#366](https://github.com/Open-EO/openeo-api/pull/366))
- Add `Connection.authenticate_oidc` for user-friendlier OIDC authentication: first try refresh token and fall back on device code flow
- Add experimental support for `array_modify` process ([Open-EO/openeo-processes#202](https://github.com/Open-EO/openeo-processes/issues/202))

### Removed

- Remove old/deprecated `Connection.authenticate_OIDC()`


## [0.5.0] - 2021-03-17

### Added

- Add namespace support to `DataCube.process`, `PGNode`, `ProcessGraphVisitor` (minor API breaking change) and related.
  Allows building process graphs with processes from non-"backend" namespaces
  ([#182](https://github.com/Open-EO/openeo-python-client/issues/182))
- `collection_items` to request collection items through a STAC API
- `paginate` as a basic method to support link-based pagination
- Add namespace support to `Connection.datacube_from_process`
- Add basic support for band name aliases in `metadata.Band` for band index lookup (EP-3670)

### Changed

- `OpenEoApiError` moved from `openeo.rest.connection` to `openeo.rest`
- Added HTML representation for `list_jobs`, `list_services`, `list_files` and for job results
- Improve refresh token handling in OIDC logic: avoid requesting refresh token
  (which can fail if OIDC client is not set up for that) when not necessary (EP-3700)
- `RESTJob.start_and_wait`: add status line when sending "start" request, and drop microsecond resolution from status lines

### Fixed

- Updated Vue Components library (solves issue with loading from slower back-ends where no result was shown)

## [0.4.10] - 2021-02-26

### Added

- Add "reflected" operator support to `ProcessBuilder`
- Add `RESTJob.get_results()`, `JobResults` and `ResultAsset` for more fine-grained batch job result handling. (EP-3739)
- Add documentation on batch job result (asset) handling and downloading

### Changed

- Mark `Connection.imagecollection` more clearly as deprecated/legacy alias of `Connection.load_collection`
- Deprecated `job_results()` and `job_logs()` on `Connection` object, it's better to work through `RESTJob` object.
- Update `DataCube.sar_backscatter` to the latest process spec: add `coefficient` argument
  and remove `orthorectify`, `rtc`. ([openeo-processes#210](https://github.com/Open-EO/openeo-processes/pull/210))

### Removed

- Remove outdated batch job result download logic left-overs
- Remove (outdated) abstract base class `openeo.job.Job`: did not add value, only caused maintenance overhead. ([#115](https://github.com/Open-EO/openeo-python-client/issues/115))


## [0.4.9] - 2021-01-29

### Added

- Make `DataCube.filter_bbox()` easier to use: allow passing a bbox tuple, list, dict or even shapely geometry directly as first positional argument or as `bbox` keyword argument.
  Handling of the legacy non-standard west-east-north-south positional argument order is preserved for now ([#136](https://github.com/Open-EO/openeo-python-client/issues/136))
- Add "band math" methods `DataCube.ln()`, `DataCube.logarithm(base)`, `DataCube.log10()` and `DataCube.log2()`
- Improved support for creating and handling parameters when defining user-defined processes (EP-3698)
- Initial Jupyter integration: add rich HTML rendering of backend metadata (collections, file formats, UDF runtimes, ...)
  ([#170](https://github.com/Open-EO/openeo-python-client/pull/170))
- add `resolution_merge` process (experimental) (EP-3687, [openeo-processes#221](https://github.com/Open-EO/openeo-processes/pull/221))
- add `sar_backscatter` process (experimental) (EP-3612, [openeo-processes#210](https://github.com/Open-EO/openeo-processes/pull/210))

### Fixed

- Fixed 'Content-Encoding' handling in `Connection.download`: client did not automatically decompress `/result`
  responses when necessary ([#175](https://github.com/Open-EO/openeo-python-client/issues/175))


## [0.4.8] - 2020-11-17

### Added
- Add `DataCube.aggregate_spatial()`

### Changed
- Get/create default `RefreshTokenStore` lazily in `Connection`
- Various documentation tweaks

## [0.4.7] - 2020-10-22

### Added
- Add support for `title`/`description`/`plan`/`budget` in `DataCube.send_job` ([#157](https://github.com/Open-EO/openeo-python-client/pull/157) / [#158](https://github.com/Open-EO/openeo-python-client/pull/158))
- Add `DataCube.to_json()` to easily get JSON representation of a DataCube
- Allow to subclass `CollectionMetadata` and preserve original type when "cloning"

### Changed
- Changed `execute_batch` to support downloading multiple files (within EP-3359, support profiling)
- Don't send None-valued `title`/`description`/`plan`/`budget` fields from `DataCube.send_job` ([#157](https://github.com/Open-EO/openeo-python-client/pull/157) / [#158](https://github.com/Open-EO/openeo-python-client/pull/158))

### Removed
- Remove duplicate and broken `Connection.list_processgraphs`

### Fixed
- Various documentation fixes and tweaks
- Avoid `merge_cubes` warning when using non-band-math `DataCube` operators


## [0.4.6] - 2020-10-15

### Added
- Add `DataCube.aggregate_temporal`
- Add initial support to download profiling information

### Changed
- Deprecated legacy functions/methods are better documented as such and link to a recommended alternative (EP-3617).
- Get/create default `AuthConfig` in Connection lazily (allows client to run in environments without existing (default) config folder)

### Deprecated
- Deprecate `zonal_statistics` in favor of `aggregate_spatial`

### Removed
- Remove support for old, non-standard `stretch_colors` process (Use `linear_scale_range` instead).


## [0.4.5] - 2020-10-01

### Added
- Also handle `dict` arguments in `dereference_from_node_arguments` (EP-3509)
- Add support for less/greater than and equal operators
- Raise warning when user defines a UDP with same id as a pre-defined one (EP-3544, [#147](https://github.com/Open-EO/openeo-python-client/pull/147))
- Add `rename_labels` support in metadata (EP-3585)
- Improve "callback" handling (sub-process graphs): add predefined callbacks for all official processes and functionality to assemble these (EP-3555, [#153](https://github.com/Open-EO/openeo-python-client/pull/153))
- Moved datacube write/save/plot utilities from udf to client (EP-3456)
- Add documentation on OpenID Connect authentication (EP-3485)

### Fixed
- Fix `kwargs` handling in `TimingLogger` decorator


## [0.4.4] - 2020-08-20

### Added
- Add `openeo-auth` command line tool to manage OpenID Connect (and basic auth) related configs (EP-3377/EP-3493)
- Support for using config files for OpenID Connect and basic auth based authentication, instead of hardcoding credentials (EP-3377/EP-3493)

### Fixed
- Fix target_band handling in `DataCube.ndvi` (EP-3496)
