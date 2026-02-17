package io.flink.connector.http.client;

import io.flink.connector.http.client.apache.ApacheHttpRequestExecutor;
import io.flink.connector.http.config.HttpSinkConfig;
import io.flink.connector.http.model.HttpExchange;
import io.flink.connector.http.model.HttpSinkRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.metrics.MetricGroup;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Submits batches of {@link HttpSinkRecord} to their target URLs and returns {@link HttpExchange}
 * results (request + response pairs).
 *
 * <p>Uses {@link HttpRequestExecutor} for the actual HTTP execution. The executor is
 * implementation-agnostic (e.g. Apache HttpClient via {@link ApacheHttpRequestExecutor}) and works
 * only with domain types ({@link HttpSinkRecord}, {@link HttpResponse}). Submits requests in
 * parallel within each batch.
 */
@Slf4j
public class HttpSinkRequestSubmitter implements Serializable {

    private final HttpRequestExecutor requestExecutor;

    /**
     * Creates a request submitter with the given configuration.
     *
     * @param httpSinkConfig HTTP sink configuration (used for HTTP client settings)
     * @param metricGroup Flink metric group for reporting metrics
     */
    public HttpSinkRequestSubmitter(HttpSinkConfig httpSinkConfig, MetricGroup metricGroup) {
        this(new ApacheHttpRequestExecutor(
                metricGroup,
                httpSinkConfig.getHttpClientConfig(),
                httpSinkConfig.getRetryConfig()));
    }

    /**
     * Creates a request submitter with the given executor. Used for testing.
     *
     * @param requestExecutor the executor to use for HTTP requests
     */
    HttpSinkRequestSubmitter(HttpRequestExecutor requestExecutor) {
        this.requestExecutor = requestExecutor;
    }

    /**
     * Submits a batch of {@link HttpSinkRecord} to their target HTTP endpoints.
     *
     * @param records the records to send (each contains its own URL, method, headers, body)
     * @return a {@link CompletableFuture} that completes with the list of {@link HttpExchange}
     *     (request + response) for each record
     */
    public CompletableFuture<List<HttpExchange>> submitRequests(List<HttpSinkRecord> records) {
        log.info("submit requests batch size - size: {}", records.size());
        List<CompletableFuture<HttpExchange>> responseFutures =
                records.parallelStream().map(this::submitRequest).toList();

        CompletableFuture<Void> allFutures =
                CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[0]));
        return allFutures.thenApply(
                v -> responseFutures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
    }

    private CompletableFuture<HttpExchange> submitRequest(HttpSinkRecord record) {
        log.info("sending http request - url: {}", record.getUrl());
        return requestExecutor
                .execute(record)
                .thenApply(response -> HttpExchange.builder().httpSinkRecord(record).response(response).build());
    }

    /**
     * Closes the request executor. Should be called when the sink writer is closed.
     */
    public void close() {
        try {
            requestExecutor.close();
        } catch (Exception e) {
            log.error("Error occurred while closing the request submitter", e);
        }
    }
}
