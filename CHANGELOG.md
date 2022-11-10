# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Added

- Jupyter integration: show process graph visualization of `DataCube` objects instead of generic `repr`.  ([#336](https://github.com/Open-EO/openeo-python-client/issues/336))

### Changed

### Removed

### Fixed


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
