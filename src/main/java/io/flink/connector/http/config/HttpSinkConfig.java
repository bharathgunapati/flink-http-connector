package io.flink.connector.http.config;

import io.flink.connector.http.HttpSink;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

/**
 * Composition of configuration for {@link HttpSink}.
 *
 * <p>Aggregates three sub-configs:
 * <ul>
 *   <li>{@link SinkWriterConfig} – batching, buffering, rate limiting</li>
 *   <li>{@link HttpClientConfig} – connection, timeouts, headers</li>
 *   <li>{@link RetryConfig} – retry behavior</li>
 * </ul>
 *
 * <p>Provides delegator methods for backward compatibility so callers can use
 * {@code httpSinkConfig.getMaxBatchSize()} instead of
 * {@code httpSinkConfig.getSinkWriterConfig().getMaxBatchSize()}.
 *
 * @see SinkWriterConfig
 * @see HttpClientConfig
 * @see RetryConfig
 */
@Builder
@Getter
@EqualsAndHashCode
@ToString
public class HttpSinkConfig implements Serializable {

    @lombok.Builder.Default
    private final SinkWriterConfig sinkWriterConfig = SinkWriterConfig.defaults();

    @lombok.Builder.Default
    private final HttpClientConfig httpClientConfig = HttpClientConfig.defaults();

    @lombok.Builder.Default
    private final RetryConfig retryConfig = RetryConfig.defaults();

    // --- Delegators for backward compatibility ---

    public int getMaxBatchSize() {
        return sinkWriterConfig.getMaxBatchSize();
    }

    public int getInitialMaxInFlightRequests() {
        return sinkWriterConfig.getInitialMaxInFlightRequests();
    }

    public int getMaxInFlightRequests() {
        return sinkWriterConfig.getMaxInFlightRequests();
    }

    public int getMaxBufferedRequests() {
        return sinkWriterConfig.getMaxBufferedRequests();
    }

    public long getMaxBatchSizeInBytes() {
        return sinkWriterConfig.getMaxBatchSizeInBytes();
    }

    public long getMaxTimeInBufferMS() {
        return sinkWriterConfig.getMaxTimeInBufferMS();
    }

    public long getMaxRecordSizeInBytes() {
        return sinkWriterConfig.getMaxRecordSizeInBytes();
    }

    public int getSinkWriterThreadPoolSize() {
        return sinkWriterConfig.getSinkWriterThreadPoolSize();
    }

    public int getIncreaseRate() {
        return sinkWriterConfig.getIncreaseRate();
    }

    public double getDecreaseFactor() {
        return sinkWriterConfig.getDecreaseFactor();
    }

    public int getRateThreshold() {
        return sinkWriterConfig.getRateThreshold();
    }

    public int getConnectTimeout() {
        return httpClientConfig.getConnectTimeoutMs();
    }

    public int getRequestTimeout() {
        return httpClientConfig.getRequestTimeoutMs();
    }

    public int getResponseTimeout() {
        return httpClientConfig.getResponseTimeoutMs();
    }

    public int getSocketTimeout() {
        return httpClientConfig.getSocketTimeoutMs();
    }

    public int getReadTimeout() {
        return httpClientConfig.getReadTimeoutMs();
    }

    public int getConnectionTTL() {
        return httpClientConfig.getConnectionTtlMs();
    }

    public int getEvictIdleConnectionTimeoutMs() {
        return httpClientConfig.getEvictIdleConnectionTimeoutMs();
    }

    public int getMaxConnections() {
        return httpClientConfig.getMaxConnections();
    }

    public int getMaxConnectionsPerHost() {
        return httpClientConfig.getMaxConnectionsPerHost();
    }

    public int getHttpClientThreadPoolSize() {
        return httpClientConfig.getHttpClientThreadPoolSize();
    }

    public String getProxyHost() {
        return httpClientConfig.getProxyHost();
    }

    public Integer getProxyPort() {
        return httpClientConfig.getProxyPort();
    }

    public String getProxyScheme() {
        return httpClientConfig.getProxyScheme();
    }

    public String getTrustStorePath() {
        return httpClientConfig.getTrustStorePath();
    }

    public String getTrustStorePassword() {
        return httpClientConfig.getTrustStorePassword();
    }

    public String getKeyStorePath() {
        return httpClientConfig.getKeyStorePath();
    }

    public String getKeyStorePassword() {
        return httpClientConfig.getKeyStorePassword();
    }

    public int getMaxRetries() {
        return retryConfig.getMaxRetries();
    }

    public long getDelayInSecs() {
        return retryConfig.getDelayInSecs();
    }

    public long getMaxDelayInSecs() {
        return retryConfig.getMaxDelayInSecs();
    }

    public String getRetryStrategy() {
        return retryConfig.getRetryStrategy();
    }

    public java.util.List<Integer> getTransientHttpStatusCodes() {
        return retryConfig.getTransientStatusCodes();
    }

    /**
     * Returns an {@link HttpSinkConfig} with all default values from the sub-configs.
     *
     * @return a new config with defaults
     */
    public static HttpSinkConfig defaults() {
        return HttpSinkConfig.builder().build();
    }
}
