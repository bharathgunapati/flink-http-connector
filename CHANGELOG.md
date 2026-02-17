# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Proxy configuration (host, port, scheme) for HTTP client
- SSL/TLS configuration (trust store, key store) for custom certificates
- Request metrics: `numRequestsSent`, `numRequestsSucceeded`, `numRequestsFailed`
- LICENSE file (Apache 2.0)
- CHANGELOG.md

## [1.0.0-SNAPSHOT] - Initial development

### Added

- HTTP Sink connector for Apache Flink DataStream API
- Async, non-blocking HTTP requests via Apache HttpClient 5
- Configurable batching, retries, and AIMD rate limiting
- Support for GET, POST, PUT, PATCH, DELETE, HEAD methods
- Per-record URL, method, headers, and body via `HttpSinkRecord`
- At-least-once delivery with Flink checkpointing
- Connection pool metrics
- Architecture tests (ArchUnit)
- Unit and integration tests
