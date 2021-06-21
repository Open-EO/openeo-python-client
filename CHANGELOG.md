# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

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

### Changed

- Disallow redirects on POST/DELETE/... requests and require status code 200 on `POST /result` requests. 
  This improves error information where `POST /result` would involve a redirect. (EP-3889)
- Class `JobLogEntry` got replaced with a more complete and re-usable `LogEntry` dict
- The following methods return a `Service` class instead of a dict: `tiled_viewing_service` in `ImageCollection`, `ImageCollectionClient` and `DataCube`, `create_service` in `Connection`

### Deprecated

- The method `remove_service` in `Connection` has been deprecated in favor of `delete_service` in the `Service` class

### Removed

### Fixed



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

## Fixed
- Fix `kwargs` handling in `TimingLogger` decorator


## [0.4.4] - 2020-08-20

### Added
- Add `openeo-auth` command line tool to manage OpenID Connect (and basic auth) related configs (EP-3377/EP-3493)
- Support for using config files for OpenID Connect and basic auth based authentication, instead of hardcoding credentials (EP-3377/EP-3493)

### Fixed
- Fix target_band handling in `DataCube.ndvi` (EP-3496)
