package io.flink.connector.http.config;

import io.flink.connector.http.client.apache.HttpClient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Configuration for the HTTP client: connection, timeouts, and default headers.
 *
 * <p>Used by {@link HttpClient} to configure connection
 * pooling, timeouts (connect, socket, response), and optional default headers applied to requests.
 *
 * @see HttpSinkConfig
 * @see SinkWriterConfig
 * @see RetryConfig
 */
@Builder
@Getter
@EqualsAndHashCode
@ToString
@AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public class HttpClientConfig implements Serializable {

    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 30000;
    private static final int DEFAULT_REQUEST_TIMEOUT_MS = 30000;
    private static final int DEFAULT_RESPONSE_TIMEOUT_MS = 30000;
    private static final int DEFAULT_SOCKET_TIMEOUT_MS = 30000;
    private static final int DEFAULT_READ_TIMEOUT_MS = 30000;
    private static final int DEFAULT_CONNECTION_TTL_MS = 30000;
    private static final int DEFAULT_EVICT_IDLE_CONNECTION_TIMEOUT_MS = 300_000; // 5 minutes
    private static final int DEFAULT_MAX_CONNECTIONS = 15;
    private static final int DEFAULT_MAX_CONNECTIONS_PER_HOST = 15;
    private static final int DEFAULT_HTTP_CLIENT_THREAD_POOL_SIZE = 64;

    private static final Map<String, Object> DEFAULT_HEADERS =
            Map.of("Content-Type", "application/json", "Accept", "application/json");

    private static final String DEFAULT_PROXY_SCHEME = "http";

    private final int connectTimeoutMs;
    private final int requestTimeoutMs;
    private final int responseTimeoutMs;
    private final int socketTimeoutMs;
    private final int readTimeoutMs;
    private final int connectionTtlMs;
    private final int evictIdleConnectionTimeoutMs;
    private final int maxConnections;
    private final int maxConnectionsPerHost;
    private final int httpClientThreadPoolSize;
    private final Map<String, Object> defaultHeaders;

    /** Proxy host (e.g. "proxy.example.com"). When null, no proxy is used. */
    @lombok.Builder.Default
    private final String proxyHost = null;

    /** Proxy port (e.g. 8080). Required when proxyHost is set. */
    @lombok.Builder.Default
    private final Integer proxyPort = null;

    /** Proxy scheme ("http" or "https"). Default "http". */
    @lombok.Builder.Default
    private final String proxyScheme = DEFAULT_PROXY_SCHEME;

    /** Path to trust store (JKS or PKCS12) for custom CA certificates. When null, JVM default is used. */
    @lombok.Builder.Default
    private final String trustStorePath = null;

    /** Trust store password. Required when trustStorePath is set. */
    @lombok.Builder.Default
    private final String trustStorePassword = null;

    /** Path to key store (JKS or PKCS12) for client certificates. When null, no client cert. */
    @lombok.Builder.Default
    private final String keyStorePath = null;

    /** Key store password. Required when keyStorePath is set. */
    @lombok.Builder.Default
    private final String keyStorePassword = null;

    public Map<String, Object> getDefaultHeaders() {
        return defaultHeaders != null ? defaultHeaders : Collections.emptyMap();
    }

    /**
     * Returns true if proxy is configured (both proxyHost and proxyPort are set).
     */
    public boolean isProxyConfigured() {
        return proxyHost != null && !proxyHost.isEmpty() && proxyPort != null;
    }

    /**
     * Returns true if custom trust store is configured.
     */
    public boolean isTrustStoreConfigured() {
        return trustStorePath != null && !trustStorePath.isEmpty();
    }

    /**
     * Returns true if custom key store (client cert) is configured.
     */
    public boolean isKeyStoreConfigured() {
        return keyStorePath != null && !keyStorePath.isEmpty();
    }

    /**
     * Returns an {@link HttpClientConfig} with all default values.
     *
     * @return default config
     */
    public static HttpClientConfig defaults() {
        return new HttpClientConfig(
                DEFAULT_CONNECT_TIMEOUT_MS,
                DEFAULT_REQUEST_TIMEOUT_MS,
                DEFAULT_RESPONSE_TIMEOUT_MS,
                DEFAULT_SOCKET_TIMEOUT_MS,
                DEFAULT_READ_TIMEOUT_MS,
                DEFAULT_CONNECTION_TTL_MS,
                DEFAULT_EVICT_IDLE_CONNECTION_TIMEOUT_MS,
                DEFAULT_MAX_CONNECTIONS,
                DEFAULT_MAX_CONNECTIONS_PER_HOST,
                DEFAULT_HTTP_CLIENT_THREAD_POOL_SIZE,
                DEFAULT_HEADERS,
                null,
                null,
                DEFAULT_PROXY_SCHEME,
                null,
                null,
                null,
                null);
    }
}
