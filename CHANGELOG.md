# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
### Changed
- Updated Elasticsearch *query_by_id* method to accept an *index* as argument
- SDAP-462: Updated query logic so that depth -99999 is treated as surface (i.e. depth 0)
- SDAP-463: Added capability to further partition parquet objects/files by platform
### Changed
### Deprecated
### Removed
### Fixed
- Fixed helm chart not building/installing
### Security

## [0.3.0] - 2022-07-13
### Added
- CDMS-xxx: Added `CLI` script to ingest S3 data into the Parquet system
- SDAP-394: added meta as default column in in-situ endpoint response
- SDAP-395: added cdms json schema endpoint
### Changed
### Deprecated
### Removed
### Fixed
### Security
