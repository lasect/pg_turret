# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-04-05

### Added

- Log capture with metadata and dynamic GUC reloading
- Kafka adapter with retry logic for `UnknownTopicOrPartition` errors
- Exponential backoff for Kafka producer retries
- Sentry integration with live config reload
- Log filtering by level and regex pattern
- Retry queue for failed log exports
- Metrics module for tracking log capture statistics
- Gzip compression support for HTTP exports
- Shared HTTP client with dynamic timeout configuration
- GUC settings for compression, workers, retry, and filtering

### Changed

- Improved mutex error handling in config module
- Consolidated mutex locks in log filtering for better performance
- Updated README with new features and configuration options

### Fixed

- Resolved PostgreSQL 18 crash on extension load
- Improved startup safety and enhanced retry queue logic

### Performance

- Batch retry queue operations for reduced overhead
- Dynamic HTTP client timeout based on configuration
- Consolidated mutex locks in log filtering
