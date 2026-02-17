package io.flink.connector.http.config;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RetryConfigTest {

    @Test
    void defaults_returnsConfigWithExpectedValues() {
        RetryConfig config = RetryConfig.defaults();

        assertEquals(3, config.getMaxRetries());
        assertEquals(5L, config.getDelayInSecs());
        assertEquals(10L, config.getMaxDelayInSecs());
        assertEquals("FIXED", config.getRetryStrategy());
        assertTrue(config.isRetryEnabled());
        assertTrue(config.getTransientStatusCodes().contains(500));
        assertTrue(config.getTransientStatusCodes().contains(429));
        assertTrue(config.getNonRetryableStatusCodes().contains(400));
    }

    @Test
    void getDelayInMillis_convertsSecondsToMillis() {
        RetryConfig config = RetryConfig.builder()
                .delayInSecs(5L)
                .build();

        assertEquals(5000L, config.getDelayInMillis());
    }

    @Test
    void getMaxRetryDelayInMillis_convertsSecondsToMillis() {
        RetryConfig config = RetryConfig.builder()
                .maxDelayInSecs(10L)
                .build();

        assertEquals(10000L, config.getMaxRetryDelayInMillis());
    }

    @Test
    void getRetryableStatusCodes_returnsTransientStatusCodes() {
        RetryConfig config = RetryConfig.defaults();

        List<Integer> retryable = config.getRetryableStatusCodes();
        List<Integer> transientCodes = config.getTransientStatusCodes();
        assertEquals(transientCodes, retryable);
    }

    @Test
    void getTransientStatusCodes_returnsDefault_whenNull() {
        RetryConfig config = RetryConfig.builder()
                .maxRetries(3)
                .delayInSecs(5L)
                .maxDelayInSecs(10L)
                .retryStrategy("FIXED")
                .retryEnabled(true)
                .transientStatusCodes(null)
                .nonRetryableStatusCodes(List.of(400))
                .build();

        assertNotNull(config.getTransientStatusCodes());
        assertFalse(config.getTransientStatusCodes().isEmpty());
    }

    @Test
    void builder_acceptsZeroMaxRetries() {
        RetryConfig config = RetryConfig.builder()
                .maxRetries(0)
                .delayInSecs(5L)
                .maxDelayInSecs(10L)
                .retryStrategy("FIXED")
                .retryEnabled(true)
                .transientStatusCodes(List.of(500, 502, 503))
                .nonRetryableStatusCodes(List.of(400))
                .build();

        assertEquals(0, config.getMaxRetries());
    }

    @Test
    void builder_acceptsRetryDisabled() {
        RetryConfig config = RetryConfig.builder()
                .retryEnabled(false)
                .build();

        assertFalse(config.isRetryEnabled());
    }

    @Test
    void builder_acceptsEmptyTransientStatusCodes() {
        RetryConfig config = RetryConfig.builder()
                .maxRetries(3)
                .delayInSecs(5L)
                .maxDelayInSecs(10L)
                .retryStrategy("FIXED")
                .retryEnabled(true)
                .transientStatusCodes(List.of())
                .nonRetryableStatusCodes(List.of(400))
                .build();

        assertNotNull(config.getTransientStatusCodes());
        assertTrue(config.getTransientStatusCodes().isEmpty());
    }

    @Test
    void builder_acceptsEmptyNonRetryableStatusCodes() {
        RetryConfig config = RetryConfig.builder()
                .maxRetries(3)
                .delayInSecs(5L)
                .maxDelayInSecs(10L)
                .retryStrategy("FIXED")
                .retryEnabled(true)
                .transientStatusCodes(List.of(500))
                .nonRetryableStatusCodes(List.of())
                .build();

        assertNotNull(config.getNonRetryableStatusCodes());
        assertTrue(config.getNonRetryableStatusCodes().isEmpty());
    }

    @Test
    void builder_acceptsNegativeDelay() {
        RetryConfig config = RetryConfig.builder()
                .delayInSecs(-1L)
                .build();

        assertEquals(-1L, config.getDelayInSecs());
        assertEquals(-1000L, config.getDelayInMillis());
    }

    @Test
    void getRetriableExceptionClasses_returnsExpectedTypes() {
        RetryConfig config = RetryConfig.defaults();
        var classes = config.getRetriableExceptionClasses();

        assertNotNull(classes);
        assertTrue(classes.size() >= 5);
        assertTrue(classes.contains(java.net.UnknownHostException.class));
        assertTrue(classes.contains(javax.net.ssl.SSLException.class));
    }
}
