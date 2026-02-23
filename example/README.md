# Flink HTTP Sink Connector Examples

Examples demonstrating usage of the flink-http-connector for different HTTP methods.

## Run

From the connector root:

### POST (default)

```bash
mvn exec:java -pl example
```

### PUT

```bash
mvn exec:java -pl example -Dexec.mainClass="io.flink.connector.http.example.HttpSinkPutExample"
```

### PATCH

```bash
mvn exec:java -pl example -Dexec.mainClass="io.flink.connector.http.example.HttpSinkPatchExample"
```

### DELETE

```bash
mvn exec:java -pl example -Dexec.mainClass="io.flink.connector.http.example.HttpSinkDeleteExample"
```

## Custom Endpoint

Override the HTTP endpoint via system property:

```bash
mvn exec:java -pl example -Dexec.systemProperties="http.endpoint.url=http://localhost:8080/ingest"
```

## Examples

| Class | Method | Default endpoint |
|-------|--------|------------------|
| `HttpSinkExample` | POST | https://httpbin.org/post |
| `HttpSinkPutExample` | PUT | https://httpbin.org/put |
| `HttpSinkPatchExample` | PATCH | https://httpbin.org/patch |
| `HttpSinkDeleteExample` | DELETE | https://httpbin.org/delete |
