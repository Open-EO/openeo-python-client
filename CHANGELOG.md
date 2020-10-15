# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased / Draft

### Added

### Changed
- Deprecated legacy functions/methods are better documented as such and link to a recommended alternative (EP-3617).

### Removed
- Remove support for old, non-standard `stretch_colors` process (Use `linear_scale_range` instead).

### Fixed

## 0.4.4 - 2020-08-20

### Added
- Add `openeo-auth` command line tool to manage OpenID Connect (and basic auth) related configs (EP-3377/EP-3493)
- Support for using config files for OpenID Connect and basic auth based authentication, instead of hardcoding credentials (EP-3377/EP-3493)

### Changed

### Removed

### Fixed
- Fix target_band handling in `DataCube.ndvi` (EP-3496)
