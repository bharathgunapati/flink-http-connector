# Flink HTTP Sink Connector

[![Java 17](https://img.shields.io/badge/Java-17+-blue.svg)](https://docs.oracle.com/en/java/javase/17/)
[![Flink 1.20](https://img.shields.io/badge/Flink-1.20.x-orange.svg)](https://flink.apache.org/)

This connector provides an HTTP sink for Apache Flink's **DataStream API** that writes stream elements to external systems via HTTP requests. Built on [Flink's AsyncSinkBase](https://nightlies.apache.org/flink/flink-docs-stable/api/java/org/apache/flink/connector/base/sink/AsyncSinkBase.html) (FLIP-171), it offers configurable batching, retries, and rate limiting.

> **Supported API**: This connector supports the **Streaming API (DataStream)** only. Table API, SQL API, and HTTP Lookup Source are not supported.

## Dependency

Apache Flink's streaming connectors are not part of the binary distribution. See how to [link connectors for cluster execution](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/configuration/overview/).

Add the following dependency to your project:

```xml
<dependency>
    <groupId>io.flink.connectors</groupId>
    <artifactId>flink-http-connector</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### Prerequisites

- **Java 17+**
- **Apache Flink 1.20.x**
- **Maven 3.6+** (for building from source)

### Runtime Dependencies

The following Flink modules are provided at runtime and should not be bundled in your job JAR:

- `org.apache.flink:flink-streaming-java`
- `org.apache.flink:flink-connector-base`

## HTTP Sink

`HttpSink` allows writing a stream of records to one or more HTTP endpoints.

### Usage

The HTTP sink provides a constructor that takes an `ElementConverter` and `HttpSinkConfig`. The following code shows how to write records to an HTTP endpoint with at-least-once delivery guarantee:

```java
import io.flink.connector.http.HttpSink;
import io.flink.connector.http.config.HttpSinkConfig;
import io.flink.connector.http.model.HttpSinkRecord;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

ElementConverter<String, HttpSinkRecord> converter = (element, context) ->
    HttpSinkRecord.builder()
        .method("POST")
        .url("https://api.example.com/ingest")
        .headers(Map.of("Content-Type", "application/json"))
        .body(Map.of("payload", element))
        .build();

HttpSink<String> sink = new HttpSink<>(converter, HttpSinkConfig.defaults());

DataStream<String> stream = env.fromElements("a", "b", "c");
stream.sinkTo(sink);

env.execute();
```

The following properties are required to build an `HttpSink`:

- **Element converter** — A function that converts stream elements to `HttpSinkRecord` (URL, method, headers, body)
- **Configuration** — `HttpSinkConfig` (defaults are used when not specified)

### Per-Record URL and Headers

Each record can target a different URL and include custom headers:

```java
ElementConverter<Event, HttpSinkRecord> converter = (event, context) ->
    HttpSinkRecord.builder()
        .method("POST")
        .url("https://api.example.com/events/" + event.getType())
        .headers(Map.of(
            "Content-Type", "application/json",
            "Authorization", "Bearer " + event.getToken(),
            "X-Request-Id", event.getId()))
        .body(Map.of("event", event.getPayload()))
        .build();
```

### Supported HTTP Methods

`HttpSinkRecord` supports: `GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `HEAD`, `OPTIONS`.

### Request Body Format

The `body` field is a `Map<String, Object>` serialized as JSON. Use `Map.of()` or `HashMap` for structured payloads.

## Configuration

All options are configured via `HttpSinkConfig.builder()` with nested `SinkWriterConfig`, `HttpClientConfig`, and `RetryConfig`. Defaults are used when not specified.

### Sink Writer (`SinkWriterConfig`)

| Option | Required | Description | Default |
|--------|----------|-------------|---------|
| `maxBatchSize` | no | Max records per batch | 500 |
| `initialMaxInFlightRequests` | no | Initial max concurrent in-flight requests | 1 |
| `maxInFlightRequests` | no | Max concurrent in-flight requests | 10 |
| `maxBufferedRequests` | no | Max buffered requests before backpressure | 10000 |
| `maxBatchSizeInBytes` | no | Max batch size in bytes | 5242880 (5 MB) |
| `maxTimeInBufferMS` | no | Max time to buffer before flush (ms) | 5000 |
| `maxRecordSizeInBytes` | no | Max size of a single record (bytes) | 1048576 (1 MB) |
| `sinkWriterThreadPoolSize` | no | Sink writer thread pool size | 4 |
| `increaseRate` | no | AIMD increase rate for congestion control | 1 |
| `decreaseFactor` | no | AIMD decrease factor for congestion control | 0.5 |
| `rateThreshold` | no | AIMD rate threshold | 10 |

### HTTP Client (`HttpClientConfig`)

| Option | Required | Description | Default |
|--------|----------|-------------|---------|
| `connectTimeoutMs` | no | Connection timeout (ms) | 30000 |
| `requestTimeoutMs` | no | Request timeout (ms) | 30000 |
| `responseTimeoutMs` | no | Response timeout (ms) | 30000 |
| `socketTimeoutMs` | no | Socket timeout (ms) | 30000 |
| `readTimeoutMs` | no | Read timeout (ms) | 30000 |
| `connectionTtlMs` | no | Connection time-to-live (ms) | 30000 |
| `evictIdleConnectionTimeoutMs` | no | Idle connection eviction timeout (ms) | 300000 (5 min) |
| `maxConnections` | no | Max total connections | 15 |
| `maxConnectionsPerHost` | no | Max connections per host | 15 |
| `httpClientThreadPoolSize` | no | HTTP client thread pool size | 64 |
| `defaultHeaders` | no | Default headers for all requests | `Content-Type: application/json`, `Accept: application/json` |

### Retry (`RetryConfig`)

| Option | Required | Description | Default |
|--------|----------|-------------|---------|
| `maxRetries` | no | Max retry attempts | 3 |
| `delayInSecs` | no | Delay between retries (seconds) | 5 |
| `maxDelayInSecs` | no | Max delay between retries (seconds) | 10 |
| `retryStrategy` | no | Retry strategy name | `FIXED` |
| `retryEnabled` | no | Enable/disable retries | true |
| `transientStatusCodes` | no | HTTP status codes that trigger retry | 500, 502, 503, 504, 408, 429 |
| `nonRetryableStatusCodes` | no | HTTP status codes that do not retry | 400 |

**Retriable exceptions** (always): `UnknownHostException`, `ConnectionClosedException`, `NoRouteToHostException`, `SSLException`, `ConnectTimeoutException`, `NoHttpResponseException`.

### Custom Configuration Example

```java
import io.flink.connector.http.config.RetryConfig;
import io.flink.connector.http.config.SinkWriterConfig;

import java.util.List;

HttpSinkConfig config = HttpSinkConfig.builder()
    .sinkWriterConfig(
        SinkWriterConfig.builder()
            .maxBatchSize(100)
            .maxTimeInBufferMS(1000)
            .maxInFlightRequests(20)
            .build())
    .retryConfig(
        RetryConfig.builder()
            .maxRetries(5)
            .retryEnabled(true)
            .transientStatusCodes(List.of(500, 502, 503, 429))
            .nonRetryableStatusCodes(List.of(400))
            .build())
    .build();

HttpSink<String> sink = new HttpSink<>(converter, config);
```

## Fault Tolerance

`HttpSink` supports **at-least-once** delivery when Flink checkpointing is enabled. The connector uses [AsyncSinkBase](https://nightlies.apache.org/flink/flink-docs-stable/api/java/org/apache/flink/connector/base/sink/AsyncSinkBase.html) buffering semantics:

- Records are buffered and batched before sending
- Checkpointing ensures buffered records are persisted and replayed on failure
- Messages may be duplicated when Flink restarts due to reprocessing

Enable checkpointing for your job:

```java
env.enableCheckpointing(5000);
```

## Building from Source

Prerequisites: Unix-like environment (Linux, macOS), Git, Maven 3.6+, Java 17.

```bash
git clone <repository-url>
cd flink-http-connector
mvn clean package -DskipTests
```

Or with tests:

```bash
mvn clean package
```

The resulting JAR can be found in the `target` directory. Integration tests (`HttpSinkTest`) use WireMock and require a Flink MiniCluster via `flink-test-utils`.

## Implementation Notes

The connector extends Flink's [AsyncSinkBase](https://nightlies.apache.org/flink/flink-docs-stable/api/java/org/apache/flink/connector/base/sink/AsyncSinkBase.html) and uses:

- **HttpSinkWriter** — Buffers and batches `HttpSinkRecord` elements, submits via Apache HttpClient 5
- **HttpSinkRecordKryoSerializer** — Checkpoint serialization for buffered requests
- **HttpRequestRetryStrategy** — Configurable retry for status codes and IO exceptions

## License

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)
