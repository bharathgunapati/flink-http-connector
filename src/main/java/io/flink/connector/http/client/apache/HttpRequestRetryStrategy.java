package io.flink.connector.http.client.apache;

import io.flink.connector.http.config.RetryConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.http.protocol.HttpCoreContext;
import org.apache.hc.core5.util.TimeValue;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Custom HTTP request retry strategy that extends {@link
 * org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy}.
 *
 * <p>Key features:
 * <ul>
 *   <li>Configurable retry behavior through {@link RetryConfig}</li>
 *   <li>Support for enabling/disabling retries dynamically</li>
 *   <li>Custom retry intervals based on configured delay and max delay</li>
 *   <li>Handling of both IO exceptions and HTTP response-based retries</li>
 * </ul>
 *
 * <p>This implementation is serializable for use in distributed Flink environments. Configuration
 * includes maximum retry attempts, delay between retries, retryable exception classes, and
 * retryable HTTP status codes.
 *
 * @see org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy
 * @see RetryConfig
 */
@Slf4j
public class HttpRequestRetryStrategy extends DefaultHttpRequestRetryStrategy
        implements Serializable {

    private final boolean isRetryEnabled;

    /**
     * Constructs a new HttpRequestRetryStrategy with the specified retry configuration.
     *
     * @param retryConfig the retry configuration. Must not be null.
     */
    public HttpRequestRetryStrategy(RetryConfig retryConfig) {
        super(
                retryConfig.getMaxRetries(),
                TimeValue.of(retryConfig.getDelayInMillis(), TimeUnit.MILLISECONDS),
                retryConfig.getRetriableExceptionClasses(),
                retryConfig.getRetryableStatusCodes());
        this.isRetryEnabled = retryConfig.isRetryEnabled();
    }

    /**
     * Determines whether to retry a request that failed due to an IOException. This method first
     * checks if retries are enabled globally, and if so, delegates to the parent class's
     * implementation for specific retry logic.
     *
     * @param request   the HTTP request that failed
     * @param exception the IOException that caused the failure
     * @param execCount the number of times the request has been executed (1-based)
     * @param context   the HTTP context containing request execution state
     * @return true if the request should be retried, false otherwise
     */
    @Override
    public boolean retryRequest(
            HttpRequest request, IOException exception, int execCount, HttpContext context) {
        if (isRetryEnabled) {
            log.debug("Retrying request, attempt: {}, uri: {}, recordId {}, exceptionMessage: {}",
                    execCount, request.getRequestUri(), getRecordIdHeader((HttpCoreContext) context), exception.getMessage());
            return super.retryRequest(request, exception, execCount, context);
        }
        return false;
    }

    private String getRecordIdHeader(HttpCoreContext context) {
        try {
            return context.getRequest().getHeader("x-zeta-perseus-record-id").getValue();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Determines whether to retry a request based on the HTTP response status code. This method first
     * checks if retries are enabled globally, and if so, delegates to the parent class's
     * implementation for specific retry logic.
     *
     * @param response  the HTTP response received from the server
     * @param execCount the number of times the request has been executed (1-based)
     * @param context   the HTTP context containing request execution state
     * @return true if the request should be retried based on the response status, false otherwise
     */
    @Override
    public boolean retryRequest(HttpResponse response, int execCount, HttpContext context) {
        if (isRetryEnabled) {
            log.info(
                    "Retrying request, attempt: {}, uri: {}, recordId: {}",
                    execCount,
                    ((HttpCoreContext) context).getRequest().getRequestUri(),
                    getRecordIdHeader((HttpCoreContext) context));
            return super.retryRequest(response, execCount, context);
        }
        return false;
    }

    /**
     * Gets the retry interval for the next retry attempt. The interval is determined by the
     * configured retry strategy, which can implement various backoff policies.
     *
     * <p>For the first retry (execCount == 1), it uses the initial retry delay. For subsequent
     * retries, it gets the next retry strategy and its delay.
     *
     * @param response  the HTTP response received from the server
     * @param execCount the number of times the request has been executed (1-based)
     * @param context   the HTTP context containing request execution state
     * @return the time interval to wait before the next retry attempt
     */
    @Override
    public TimeValue getRetryInterval(HttpResponse response, int execCount, HttpContext context) {
        return super.getRetryInterval(response, execCount, context);
    }

    /**
     * Indicates whether the request should be handled as idempotent. This implementation treats all
     * requests as idempotent to ensure safe retries. This is a conservative approach that may be
     * overridden in subclasses if specific request types need different handling.
     *
     * @param request the HTTP request to be evaluated
     * @return true to indicate the request should be treated as idempotent
     */
    @Override
    protected boolean handleAsIdempotent(HttpRequest request) {
        return true;
    }
}
