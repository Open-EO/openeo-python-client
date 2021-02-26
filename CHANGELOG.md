# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

### Fixed


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
