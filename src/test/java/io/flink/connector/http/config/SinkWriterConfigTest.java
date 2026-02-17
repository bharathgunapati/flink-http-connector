package io.flink.connector.http.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SinkWriterConfigTest {

    @Test
    void defaults_returnsConfigWithExpectedValues() {
        SinkWriterConfig config = SinkWriterConfig.defaults();

        assertEquals(500, config.getMaxBatchSize());
        assertEquals(1, config.getInitialMaxInFlightRequests());
        assertEquals(10, config.getMaxInFlightRequests());
        assertEquals(10000, config.getMaxBufferedRequests());
        assertEquals(5L * 1024 * 1024, config.getMaxBatchSizeInBytes());
        assertEquals(5000, config.getMaxTimeInBufferMS());
        assertEquals(1024 * 1024L, config.getMaxRecordSizeInBytes());
        assertEquals(4, config.getSinkWriterThreadPoolSize());
        assertEquals(1, config.getIncreaseRate());
        assertEquals(0.5, config.getDecreaseFactor());
        assertEquals(10, config.getRateThreshold());
    }

    @Test
    void builder_createsConfigWithCustomValues() {
        SinkWriterConfig config = SinkWriterConfig.builder()
                .maxBatchSize(100)
                .maxInFlightRequests(20)
                .build();

        assertEquals(100, config.getMaxBatchSize());
        assertEquals(20, config.getMaxInFlightRequests());
    }

    @Test
    void builder_acceptsZeroMaxBatchSize() {
        SinkWriterConfig config =
                SinkWriterConfig.builder().maxBatchSize(0).build();

        assertEquals(0, config.getMaxBatchSize());
    }

    @Test
    void builder_acceptsZeroMaxTimeInBuffer() {
        SinkWriterConfig config =
                SinkWriterConfig.builder().maxTimeInBufferMS(0).build();

        assertEquals(0, config.getMaxTimeInBufferMS());
    }

    @Test
    void builder_acceptsZeroIncreaseRate() {
        SinkWriterConfig config =
                SinkWriterConfig.builder().increaseRate(0).build();

        assertEquals(0, config.getIncreaseRate());
    }

    @Test
    void builder_acceptsNegativeDecreaseFactor() {
        SinkWriterConfig config =
                SinkWriterConfig.builder().decreaseFactor(-0.5).build();

        assertEquals(-0.5, config.getDecreaseFactor());
    }

    @Test
    void builder_acceptsZeroMaxInFlightRequests() {
        SinkWriterConfig config =
                SinkWriterConfig.builder().maxInFlightRequests(0).build();

        assertEquals(0, config.getMaxInFlightRequests());
    }
}
