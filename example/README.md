# Flink HTTP Sink Connector Example

Example demonstrating usage of the flink-http-connector.

## Run

From the connector root:

```bash
mvn exec:java -pl example
```

Or with a custom endpoint:

```bash
mvn exec:java -pl example -Dexec.systemProperties="http.endpoint.url=http://localhost:8080/ingest"
```
