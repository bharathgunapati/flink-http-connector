package io.flink.connector.http.client.apache;

import io.flink.connector.http.config.RetryConfig;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.message.BasicHttpRequest;
import org.apache.hc.core5.http.message.BasicHttpResponse;
import org.apache.hc.core5.http.protocol.HttpCoreContext;
import org.apache.hc.core5.util.TimeValue;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HttpRequestRetryStrategyTest {

    private static final String REQUEST_URI = "http://example.com/ingest";

    @Test
    void retryRequest_withRetryableExceptionAndRetriesEnabled_returnsTrue() {
        RetryConfig config = RetryConfig.builder()
                .retryEnabled(true)
                .maxRetries(3)
                .delayInSecs(1L)
                .maxDelayInSecs(5L)
                .retryStrategy("FIXED")
                .transientStatusCodes(List.of(500, 502, 503))
                .nonRetryableStatusCodes(List.of(400))
                .build();

        HttpRequestRetryStrategy httpRequestRetryStrategy = new HttpRequestRetryStrategy(config);
        HttpRequest request = new BasicHttpRequest("POST", REQUEST_URI);
        HttpCoreContext context = new HttpCoreContext();
        context.setRequest(request);

        IOException exception = new UnknownHostException("unknown host");
        boolean shouldRetry = httpRequestRetryStrategy.retryRequest(request, exception, 1, context);

        assertFalse(shouldRetry);
    }

    @Test
    void retryRequest_withRetriesDisabled_returnsFalse() {
        RetryConfig config = RetryConfig.builder()
                .retryEnabled(false)
                .maxRetries(3)
                .delayInSecs(1L)
                .maxDelayInSecs(5L)
                .retryStrategy("FIXED")
                .transientStatusCodes(List.of(500))
                .nonRetryableStatusCodes(List.of(400))
                .build();

        HttpRequestRetryStrategy strategy = new HttpRequestRetryStrategy(config);
        HttpRequest request = new BasicHttpRequest("POST", REQUEST_URI);
        HttpCoreContext context = new HttpCoreContext();
        context.setRequest(request);

        IOException exception = new UnknownHostException("unknown host");
        boolean shouldRetry = strategy.retryRequest(request, exception, 1, context);

        assertFalse(shouldRetry);
    }

    @Test
    void retryRequest_with500ResponseAndRetriesEnabled_returnsTrue() {
        RetryConfig config = RetryConfig.builder()
                .retryEnabled(true)
                .maxRetries(3)
                .delayInSecs(1L)
                .maxDelayInSecs(5L)
                .retryStrategy("FIXED")
                .transientStatusCodes(List.of(500, 502, 503))
                .nonRetryableStatusCodes(List.of(400))
                .build();

        HttpRequestRetryStrategy strategy = new HttpRequestRetryStrategy(config);
        HttpRequest request = new BasicHttpRequest("POST", REQUEST_URI);
        HttpResponse response = new BasicHttpResponse(500, "Internal Server Error");
        HttpCoreContext context = new HttpCoreContext();
        context.setRequest(request);

        boolean shouldRetry = strategy.retryRequest(response, 1, context);

        assertTrue(shouldRetry);
    }

    @Test
    void retryRequest_with400Response_returnsFalse() {
        RetryConfig config = RetryConfig.defaults();

        HttpRequestRetryStrategy strategy = new HttpRequestRetryStrategy(config);
        HttpRequest request = new BasicHttpRequest("POST", REQUEST_URI);
        HttpResponse response = new BasicHttpResponse(400, "Bad Request");
        HttpCoreContext context = new HttpCoreContext();
        context.setRequest(request);

        boolean shouldRetry = strategy.retryRequest(response, 1, context);

        assertFalse(shouldRetry);
    }

    @Test
    void retryRequest_with500ResponseAndRetriesDisabled_returnsFalse() {
        RetryConfig config = RetryConfig.builder()
                .retryEnabled(false)
                .maxRetries(3)
                .delayInSecs(1L)
                .maxDelayInSecs(5L)
                .retryStrategy("FIXED")
                .transientStatusCodes(List.of(500))
                .nonRetryableStatusCodes(List.of(400))
                .build();

        HttpRequestRetryStrategy strategy = new HttpRequestRetryStrategy(config);
        HttpRequest request = new BasicHttpRequest("POST", REQUEST_URI);
        HttpResponse response = new BasicHttpResponse(500, "Internal Server Error");
        HttpCoreContext context = new HttpCoreContext();
        context.setRequest(request);

        boolean shouldRetry = strategy.retryRequest(response, 1, context);

        assertFalse(shouldRetry);
    }

    @Test
    void retryRequest_with429Response_returnsTrue() {
        RetryConfig config = RetryConfig.defaults();

        HttpRequestRetryStrategy strategy = new HttpRequestRetryStrategy(config);
        HttpRequest request = new BasicHttpRequest("POST", REQUEST_URI);
        HttpResponse response = new BasicHttpResponse(429, "Too Many Requests");
        HttpCoreContext context = new HttpCoreContext();
        context.setRequest(request);

        boolean shouldRetry = strategy.retryRequest(response, 1, context);

        assertTrue(shouldRetry);
    }

    @Test
    void getRetryInterval_returnsConfiguredDelay() {
        RetryConfig config = RetryConfig.builder()
                .delayInSecs(2L)
                .maxDelayInSecs(10L)
                .retryStrategy("FIXED")
                .transientStatusCodes(List.of(500))
                .nonRetryableStatusCodes(List.of(400))
                .build();

        HttpRequestRetryStrategy strategy = new HttpRequestRetryStrategy(config);
        HttpRequest request = new BasicHttpRequest("POST", REQUEST_URI);
        HttpResponse response = new BasicHttpResponse(500, "Internal Server Error");
        HttpCoreContext context = new HttpCoreContext();
        context.setRequest(request);

        TimeValue interval = strategy.getRetryInterval(response, 1, context);

        assertNotNull(interval);
        assertEquals(2000, interval.getDuration());
    }
}
