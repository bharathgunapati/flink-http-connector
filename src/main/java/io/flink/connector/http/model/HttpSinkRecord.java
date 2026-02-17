package io.flink.connector.http.model;

import io.flink.connector.http.HttpSink;
import io.flink.connector.http.util.JsonSerde;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Represents a single HTTP request for the HTTP sink. Aligns with standard HTTP request semantics
 * used by widely available HTTP clients (Apache HttpClient, Java HttpClient, OkHttp).
 *
 * <p>All fields mirror the core components of an HTTP request: method, URL, headers, and body.
 * Body is a {@code Map<String, Object>} to support variable replacement (e.g. via Nashorn) when
 * building URLs and request payloads.
 *
 * <p>This is the internal record type produced by the {@link HttpSink}
 * element converter and consumed by the sink writer.
 *
 * @see HttpSink
 * @see HttpExchange
 */
@Builder
@Getter
@EqualsAndHashCode
@ToString
public final class HttpSinkRecord implements Serializable {

    /**
     * HTTP method: GET, POST, PUT, PATCH, DELETE, HEAD.
     */
    private final String method;

    /**
     * Full request URL (scheme + host + path + query).
     */
    private final String url;

    /**
     * Request headers (e.g. Content-Type, Authorization). Null or empty if none.
     */
    private final Map<String, String> headers;

    /**
     * Request body as a map. Null or empty for GET, DELETE, HEAD. Supports variable replacement
     * (e.g. via Nashorn) when building URLs and payloads.
     */
    private final Map<String, Object> body;

    /**
     * Returns the size of the request body in bytes when serialized as JSON.
     */
    public long getSizeInBytes() {
        return JsonSerde.sizeInBytes(getBody());
    }

    /**
     * Returns headers, or empty map if null.
     */
    public Map<String, String> getHeaders() {
        return headers != null ? headers : Collections.emptyMap();
    }

    /**
     * Returns body, or empty map if null.
     */
    public Map<String, Object> getBody() {
        return body != null ? body : Collections.emptyMap();
    }
}
