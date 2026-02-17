package io.flink.connector.http.client;

import io.flink.connector.http.model.HttpResponse;
import io.flink.connector.http.model.HttpSinkRecord;

import java.util.concurrent.CompletableFuture;

/**
 * Abstraction for executing HTTP requests. Implementations may use different underlying clients
 * (e.g. Apache HttpClient, OkHttp).
 *
 * <p>Works exclusively with domain types ({@link HttpSinkRecord}, {@link HttpResponse}) so that
 * higher-level code remains implementation-agnostic.
 */
public interface HttpRequestExecutor {

    /**
     * Executes an HTTP request asynchronously.
     *
     * @param request the request to send
     * @return a future that completes with the response, or an error response (e.g. with exception
     *     message as body) if the request fails
     */
    CompletableFuture<HttpResponse> execute(HttpSinkRecord request);

    /**
     * Closes the executor and releases resources.
     */
    void close();
}
