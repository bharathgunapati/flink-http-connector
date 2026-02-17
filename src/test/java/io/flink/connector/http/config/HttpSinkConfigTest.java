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
}
