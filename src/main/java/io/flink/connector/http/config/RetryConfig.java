package io.flink.connector.http.config;

import io.flink.connector.http.client.apache.HttpRequestRetryStrategy;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.hc.client5.http.ConnectTimeoutException;
import org.apache.hc.core5.http.ConnectionClosedException;
import org.apache.hc.core5.http.NoHttpResponseException;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.Serializable;

import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;

/**
 * Configuration for retry behavior: when and how to retry failed requests.
 *
 * <p>Used by {@link HttpRequestRetryStrategy} to determine
 * maximum retries, delay between retries, which HTTP status codes and exception types trigger a
 * retry, and whether retries are enabled.
 *
 * @see HttpSinkConfig
 * @see SinkWriterConfig
 * @see HttpClientConfig
 */
@Builder
@Getter
@EqualsAndHashCode
@ToString
@AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public class RetryConfig implements Serializable {

    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long DEFAULT_DELAY_IN_SECS = 5L;
    private static final long DEFAULT_MAX_DELAY_IN_SECS = 10L;
    private static final String DEFAULT_RETRY_STRATEGY = "FIXED";
    private static final boolean DEFAULT_IS_RETRY_ENABLED = true;

    private static final List<Integer> DEFAULT_TRANSIENT_STATUS_CODES =
            List.of(500, 502, 503, 504, 408, 429);

    private static final List<Integer> DEFAULT_NON_RETRYABLE_STATUS_CODES = List.of(400);

    private static final Collection<Class<? extends IOException>> RETRIABLE_EXCEPTION_CLASSES =
            List.of(
                    UnknownHostException.class,
                    ConnectionClosedException.class,
                    NoRouteToHostException.class,
                    SSLException.class,
                    ConnectTimeoutException.class,
                    NoHttpResponseException.class);

    private final int maxRetries;
    private final long delayInSecs;
    private final long maxDelayInSecs;
    private final String retryStrategy;
    @Getter
    private final boolean retryEnabled;
    private final List<Integer> transientStatusCodes;
    private final List<Integer> nonRetryableStatusCodes;

    public List<Integer> getTransientStatusCodes() {
        return transientStatusCodes != null ? transientStatusCodes : DEFAULT_TRANSIENT_STATUS_CODES;
    }

    public List<Integer> getNonRetryableStatusCodes() {
        return nonRetryableStatusCodes != null ? nonRetryableStatusCodes : DEFAULT_NON_RETRYABLE_STATUS_CODES;
    }

    /** Delay between retries in milliseconds. */
    public long getDelayInMillis() {
        return delayInSecs * 1000L;
    }

    /** Maximum delay between retries in milliseconds. */
    public long getMaxRetryDelayInMillis() {
        return maxDelayInSecs * 1000L;
    }

    /** Status codes that trigger a retry (alias for getTransientStatusCodes). */
    public List<Integer> getRetryableStatusCodes() {
        return getTransientStatusCodes();
    }

    /** Exception classes that trigger a retry. */
    public Collection<Class<? extends IOException>> getRetriableExceptionClasses() {
        return RETRIABLE_EXCEPTION_CLASSES;
    }

    /**
     * Returns a {@link RetryConfig} with all default values.
     *
     * @return default config
     */
    public static RetryConfig defaults() {
        return new RetryConfig(
                DEFAULT_MAX_RETRIES,
                DEFAULT_DELAY_IN_SECS,
                DEFAULT_MAX_DELAY_IN_SECS,
                DEFAULT_RETRY_STRATEGY,
                DEFAULT_IS_RETRY_ENABLED,
                DEFAULT_TRANSIENT_STATUS_CODES,
                DEFAULT_NON_RETRYABLE_STATUS_CODES);
    }
}
