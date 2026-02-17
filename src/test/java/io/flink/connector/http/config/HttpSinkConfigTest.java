package io.flink.connector.http.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HttpSinkConfigTest {

    @Test
    void defaults_returnsConfigWithSubConfigDefaults() {
        HttpSinkConfig config = HttpSinkConfig.defaults();

        assertNotNull(config.getSinkWriterConfig());
        assertNotNull(config.getHttpClientConfig());
        assertNotNull(config.getRetryConfig());
    }

    @Test
    void delegators_forwardToSubConfigs() {
        HttpSinkConfig config = HttpSinkConfig.defaults();

        assertEquals(config.getSinkWriterConfig().getMaxBatchSize(), config.getMaxBatchSize());
        assertEquals(config.getHttpClientConfig().getConnectTimeoutMs(), config.getConnectTimeout());
        assertEquals(config.getRetryConfig().getMaxRetries(), config.getMaxRetries());
    }

    @Test
    void builder_createsConfigWithCustomSubConfigs() {
        SinkWriterConfig sinkConfig = SinkWriterConfig.builder()
                .maxBatchSize(100)
                .build();
        HttpSinkConfig config = HttpSinkConfig.builder()
                .sinkWriterConfig(sinkConfig)
                .build();

        assertEquals(100, config.getMaxBatchSize());
    }

    @Test
    void delegators_sinkWriterConfig_coverAllMethods() {
        HttpSinkConfig config = HttpSinkConfig.defaults();

        assertEquals(config.getSinkWriterConfig().getMaxBatchSize(), config.getMaxBatchSize());
        assertEquals(config.getSinkWriterConfig().getInitialMaxInFlightRequests(), config.getInitialMaxInFlightRequests());
        assertEquals(config.getSinkWriterConfig().getMaxInFlightRequests(), config.getMaxInFlightRequests());
        assertEquals(config.getSinkWriterConfig().getMaxBufferedRequests(), config.getMaxBufferedRequests());
        assertEquals(config.getSinkWriterConfig().getMaxBatchSizeInBytes(), config.getMaxBatchSizeInBytes());
        assertEquals(config.getSinkWriterConfig().getMaxTimeInBufferMS(), config.getMaxTimeInBufferMS());
        assertEquals(config.getSinkWriterConfig().getMaxRecordSizeInBytes(), config.getMaxRecordSizeInBytes());
        assertEquals(config.getSinkWriterConfig().getSinkWriterThreadPoolSize(), config.getSinkWriterThreadPoolSize());
        assertEquals(config.getSinkWriterConfig().getIncreaseRate(), config.getIncreaseRate());
        assertEquals(config.getSinkWriterConfig().getDecreaseFactor(), config.getDecreaseFactor());
        assertEquals(config.getSinkWriterConfig().getRateThreshold(), config.getRateThreshold());
    }

    @Test
    void delegators_httpClientConfig_coverAllMethods() {
        HttpSinkConfig config = HttpSinkConfig.defaults();

        assertEquals(config.getHttpClientConfig().getConnectTimeoutMs(), config.getConnectTimeout());
        assertEquals(config.getHttpClientConfig().getRequestTimeoutMs(), config.getRequestTimeout());
        assertEquals(config.getHttpClientConfig().getResponseTimeoutMs(), config.getResponseTimeout());
        assertEquals(config.getHttpClientConfig().getSocketTimeoutMs(), config.getSocketTimeout());
        assertEquals(config.getHttpClientConfig().getReadTimeoutMs(), config.getReadTimeout());
        assertEquals(config.getHttpClientConfig().getConnectionTtlMs(), config.getConnectionTTL());
        assertEquals(config.getHttpClientConfig().getEvictIdleConnectionTimeoutMs(), config.getEvictIdleConnectionTimeoutMs());
        assertEquals(config.getHttpClientConfig().getMaxConnections(), config.getMaxConnections());
        assertEquals(config.getHttpClientConfig().getMaxConnectionsPerHost(), config.getMaxConnectionsPerHost());
        assertEquals(config.getHttpClientConfig().getHttpClientThreadPoolSize(), config.getHttpClientThreadPoolSize());
        assertEquals(config.getHttpClientConfig().getProxyHost(), config.getProxyHost());
        assertEquals(config.getHttpClientConfig().getProxyPort(), config.getProxyPort());
        assertEquals(config.getHttpClientConfig().getProxyScheme(), config.getProxyScheme());
        assertEquals(config.getHttpClientConfig().getTrustStorePath(), config.getTrustStorePath());
        assertEquals(config.getHttpClientConfig().getTrustStorePassword(), config.getTrustStorePassword());
        assertEquals(config.getHttpClientConfig().getKeyStorePath(), config.getKeyStorePath());
        assertEquals(config.getHttpClientConfig().getKeyStorePassword(), config.getKeyStorePassword());
    }

    @Test
    void delegators_retryConfig_coverAllMethods() {
        HttpSinkConfig config = HttpSinkConfig.defaults();

        assertEquals(config.getRetryConfig().getMaxRetries(), config.getMaxRetries());
        assertEquals(config.getRetryConfig().getDelayInSecs(), config.getDelayInSecs());
        assertEquals(config.getRetryConfig().getMaxDelayInSecs(), config.getMaxDelayInSecs());
        assertEquals(config.getRetryConfig().getRetryStrategy(), config.getRetryStrategy());
        assertEquals(config.getRetryConfig().getTransientStatusCodes(), config.getTransientHttpStatusCodes());
    }

    @Test
    void builder_withCustomHttpClientConfig_delegatesCorrectly() {
        HttpClientConfig httpConfig = HttpClientConfig.builder()
                .connectTimeoutMs(10000)
                .maxConnections(25)
                .build();
        HttpSinkConfig config = HttpSinkConfig.builder()
                .httpClientConfig(httpConfig)
                .build();

        assertEquals(10000, config.getConnectTimeout());
        assertEquals(25, config.getMaxConnections());
    }

    @Test
    void builder_withCustomRetryConfig_delegatesCorrectly() {
        RetryConfig retryConfig = RetryConfig.builder()
                .maxRetries(5)
                .retryEnabled(false)
                .build();
        HttpSinkConfig config = HttpSinkConfig.builder()
                .retryConfig(retryConfig)
                .build();

        assertEquals(5, config.getMaxRetries());
        assertFalse(config.getRetryConfig().isRetryEnabled());
    }

    @Test
    void equalsAndHashCode_workCorrectly() {
        HttpSinkConfig c1 = HttpSinkConfig.defaults();
        HttpSinkConfig c2 = HttpSinkConfig.defaults();

        assertEquals(c1, c2);
        assertEquals(c1.hashCode(), c2.hashCode());
    }

    @Test
    void toString_returnsNonEmptyString() {
        HttpSinkConfig config = HttpSinkConfig.defaults();
        String str = config.toString();

        assertNotNull(str);
        assertFalse(str.isEmpty());
    }
}
