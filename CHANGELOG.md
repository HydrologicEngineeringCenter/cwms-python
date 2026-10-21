# Changelog

## [1.1.5](https://github.com/HydrologicEngineeringCenter/cwms-python/compare/v1.1.4...v1.1.5) (2026-10-21)


### Bug Fixes

* Optimize time-series retrieval defaults ([3b4f613](https://github.com/HydrologicEngineeringCenter/cwms-python/commit/3b4f6131786e96bf3bdf3e36aaeea48f5bf48063))

## [1.1.4](https://github.com/HydrologicEngineeringCenter/cwms-python/compare/v1.1.3...v1.1.4) (2026-10-15)


### Bug Fixes

* Update pandas and numpy dependencies in pyproject.toml ([#330](https://github.com/HydrologicEngineeringCenter/cwms-python/issues/330)) ([cd8903e](https://github.com/HydrologicEngineeringCenter/cwms-python/commit/cd8903e9e6e9b3d5b5408a43e78773487dacb287))

## [1.1.3](https://github.com/HydrologicEngineeringCenter/cwms-python/compare/v1.1.2...v1.1.3) (2026-10-07)


### Bug Fixes

* update to allow pandas 3.X versions with python 3.10+ ([#322](https://github.com/HydrologicEngineeringCenter/cwms-python/issues/322)) ([18bea55](https://github.com/HydrologicEngineeringCenter/cwms-python/commit/18bea55d177ab34dd339bf2048d7ce15b0483e8c))

## [1.1.2](https://github.com/HydrologicEngineeringCenter/cwms-python/compare/v1.1.1...v1.1.2) (2026-09-28)


### Bug Fixes

* configure HTTP connection pools and surface CDA batch errors ([#312](https://github.com/HydrologicEngineeringCenter/cwms-python/issues/312)) ([dd32eb1](https://github.com/HydrologicEngineeringCenter/cwms-python/commit/dd32eb1b7f4e3fe9b89495e2f02a1152d22e3341))

## [1.1.1](https://github.com/HydrologicEngineeringCenter/cwms-python/compare/v1.1.0...v1.1.1) (2026-09-28)


### Bug Fixes

* increase default time-series chunk window for multithreading...infers chunk size from interval: ([527795b](https://github.com/HydrologicEngineeringCenter/cwms-python/commit/527795b49303ec7960a69355e3353d2a1370f6d1))

## [1.1.0](https://github.com/HydrologicEngineeringCenter/cwms-python/compare/v1.0.10...v1.1.0) (2026-09-25)


### Features

* REGI-499: Project Sublocation Endpoints ([#306](https://github.com/HydrologicEngineeringCenter/cwms-python/issues/306)) ([1a0d115](https://github.com/HydrologicEngineeringCenter/cwms-python/commit/1a0d1150e6e7b1dd109ea24c5aef9f846cb1081a))

## [1.0.10](https://github.com/HydrologicEngineeringCenter/cwms-python/compare/v1.0.9...v1.0.10) (2026-09-11)


### Bug Fixes

* REGI-501: Gate Changes ([#307](https://github.com/HydrologicEngineeringCenter/cwms-python/issues/307)) ([ae76054](https://github.com/HydrologicEngineeringCenter/cwms-python/commit/ae7605424ad463017b6c08b5e5d9a2b71569ec32))

## [1.0.9](https://github.com/HydrologicEngineeringCenter/cwms-python/compare/v1.0.8...v1.0.9) (2026-09-10)


### Bug Fixes

* bump requests, black, pytest ([c37a4fe](https://github.com/HydrologicEngineeringCenter/cwms-python/commit/c37a4fef3918d24f09e620750682b46ccf62d346))
* initialize time series before concurrent chunk writes ([#310](https://github.com/HydrologicEngineeringCenter/cwms-python/issues/310)) ([d78898f](https://github.com/HydrologicEngineeringCenter/cwms-python/commit/d78898fcf573acc7c845d4f3ac735606e712a9a3))
