package io.flink.connector.http.client.apache;

import io.flink.connector.http.config.HttpClientConfig;
import io.flink.connector.http.config.RetryConfig;
import io.flink.connector.http.client.HttpRequestExecutor;
import io.flink.connector.http.model.HttpResponse;
import io.flink.connector.http.model.HttpSinkRecord;
import io.flink.connector.http.util.JsonSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.metrics.MetricGroup;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;

import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;

/**
 * {@link HttpRequestExecutor} implementation using Apache HttpClient 5.
 *
 * <p>Converts {@link HttpSinkRecord} to Apache's {@link SimpleHttpRequest}, executes via
 * {@link HttpClient}, and converts the response to {@link HttpResponse}.
 */
@Slf4j
public class ApacheHttpRequestExecutor implements HttpRequestExecutor {

    private final HttpClient httpClient;

    public ApacheHttpRequestExecutor(
            MetricGroup metricGroup,
            HttpClientConfig httpClientConfig,
            RetryConfig retryConfig) {
        this.httpClient = new HttpClient(metricGroup, httpClientConfig, retryConfig);
    }

    @Override
    public CompletableFuture<HttpResponse> execute(HttpSinkRecord request) {
        SimpleHttpRequest simpleHttpRequest = buildSimpleHttpRequest(request);
        CompletableFuture<HttpResponse> future = new CompletableFuture<>();

        httpClient.executeRequest(simpleHttpRequest, new FutureCallback<SimpleHttpResponse>() {
            @Override
            public void completed(SimpleHttpResponse simpleHttpResponse) {
                if (simpleHttpResponse == null) {
                    future.complete(HttpResponse.builder().body("No response").build());
                    return;
                }
                if (simpleHttpResponse.getCode() >= 400 && simpleHttpResponse.getCode() <= 599) {
                    log.error(
                            "received 4xx or 5xx status code - statusCode: {}, body: {}, uri: {}",
                            simpleHttpResponse.getCode(),
                            simpleHttpResponse.getBody(),
                            getUri(simpleHttpRequest));
                }
                future.complete(toHttpResponse(simpleHttpResponse));
            }

            @Override
            public void failed(Exception e) {
                log.error("http request failed - uri: {}", getUri(simpleHttpRequest), e);
                future.complete(
                        HttpResponse.builder()
                                .body(e != null ? e.getMessage() : null)
                                .build());
            }

            @Override
            public void cancelled() {
                log.error("http request cancelled");
                future.complete(HttpResponse.builder().body("Request cancelled").build());
            }
        });

        return future;
    }

    @Override
    public void close() {
        httpClient.close();
    }

    private SimpleHttpRequest buildSimpleHttpRequest(HttpSinkRecord record) {
        String encodedUrl = encodeCurlyBracesIfPresent(record.getUrl());
        String httpMethod = record.getMethod();

        SimpleHttpRequest simpleHttpRequest = new SimpleHttpRequest(httpMethod, encodedUrl);

        switch (httpMethod) {
            case "GET", "DELETE", "HEAD":
                break;
            case "POST", "PUT", "PATCH":
                String body = JsonSerde.toJson(record.getBody());
                ContentType contentType = getContentType(record);
                simpleHttpRequest.setBody(body, contentType);
                break;
            default:
                throw new IllegalArgumentException("Invalid HTTP method: " + httpMethod);
        }
        record.getHeaders().forEach(simpleHttpRequest::setHeader);
        return simpleHttpRequest;
    }

    private ContentType getContentType(HttpSinkRecord record) {
        String contentTypeHeader = record.getHeaders().get("Content-Type");
        if (contentTypeHeader != null && !contentTypeHeader.isEmpty()) {
            return ContentType.parse(contentTypeHeader);
        }
        return ContentType.APPLICATION_JSON;
    }

    /**
     * Encodes curly braces in the URL if present. Apache HTTP Client's URI parser rejects raw '{'
     * and '}' characters.
     */
    private String encodeCurlyBracesIfPresent(String url) {
        if (url == null || (url.indexOf('{') < 0 && url.indexOf('}') < 0)) {
            return url;
        }
        String encodedUrl = url.replace("{", "%7B").replace("}", "%7D");
        log.warn(
                "Encoding curly braces in HTTP URL - originalUrl: {}, encodedUrl: {}",
                url,
                encodedUrl);
        return encodedUrl;
    }

    private HttpResponse toHttpResponse(SimpleHttpResponse response) {
        return HttpResponse.builder()
                .statusCode(response.getCode())
                .body(response.getBodyText())
                .build();
    }

    private String getUri(SimpleHttpRequest request) {
        try {
            return request.getUri().toString();
        } catch (URISyntaxException e) {
            return null;
        }
    }
}
