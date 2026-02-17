# flink-http-connector

Flink HTTP transform connector — HTTP transform operator extracted from perseus-http-operator, with all Zeta/crux-iam dependencies removed.

## Features

- **HttpTransformOperator** — abstract operator for HTTP request/response transform
- **Async I/O** — uses Flink's `AsyncDataStream.unorderedWait` for non-blocking HTTP calls
- **Pluggable auth** — `AuthTokenProvider` interface for custom auth (OAuth, API keys)
- **No proprietary deps** — Apache Flink, HttpClient 5, Jackson, Gson only

## Requirements

- Java 17+
- Apache Flink 1.20.x
- Maven 3.6+

## Usage

Extend `HttpTransformOperator` and implement `buildHttpRequest` and `merge`:

```java
public class MyHttpTransformOperator extends HttpTransformOperator {

    public MyHttpTransformOperator(OperatorSpec operatorSpec) {
        super(operatorSpec);
    }

    @Override
    public HTTPRequest buildHttpRequest(HttpRecord inputRecord, OperatorSpec operatorSpec) {
        Map<String, Object> data = inputRecord.getData(Map.class);
        String url = "https://api.example.com/process?id=" + data.get("id");
        return HTTPRequest.builder()
                .url(url)
                .method("GET")
                .headers(Map.of("Accept", "application/json"))
                .build();
    }

    @Override
    public HttpRecord merge(HttpRecord in, HttpResponse response) {
        Map<String, Object> merged = new HashMap<>(in.getData(Map.class));
        merged.put("apiResponse", response.getBody());
        merged.put("statusCode", response.getStatusCode());
        in.setData(merged);
        return in;
    }
}
```

Apply to a stream:

```java
DataStream<HttpRecord> input = env.fromElements(records...);
OperatorSpec spec = ...;
DataStream<HttpRecord> output = new MyHttpTransformOperator(spec).apply(input);
```

## Build

```bash
cd /Users/bharath.re/codebase/flink-http-connector
mvn clean package
```

## Package

All code is under `io.flink.connectors.http` — no Zeta or GetInData packages.
