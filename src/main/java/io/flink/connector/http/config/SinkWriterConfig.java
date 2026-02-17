package io.flink.connector.http.config;

import io.flink.connector.http.sink.writer.HttpSinkWriter;
import io.flink.connector.http.model.HttpSinkRecord;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

/**
 * Configuration for the sink writer: batching, buffering, and rate limiting.
 *
 * <p>Controls how {@link HttpSinkWriter} buffers and batches
 * {@link HttpSinkRecord} elements before sending them as
 * HTTP requests. Includes AIMD-based congestion control parameters for adaptive in-flight request
 * limits.
 *
 * @see HttpSinkConfig
 * @see HttpClientConfig
 * @see RetryConfig
 */
@Builder
@Getter
@EqualsAndHashCode
@ToString
@AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public class SinkWriterConfig implements Serializable {

    private static final int DEFAULT_MAX_BATCH_SIZE = 500;
    private static final int DEFAULT_INITIAL_MAX_IN_FLIGHT_REQUESTS = 1;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 10;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 10000;
    private static final long DEFAULT_MAX_BATCH_SIZE_IN_BYTES = 5L * 1024 * 1024;
    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5000;
    private static final long DEFAULT_MAX_RECORD_SIZE_IN_BYTES = 1024 * 1024L;
    private static final int DEFAULT_SINK_WRITER_THREAD_POOL_SIZE = 4;
    private static final int DEFAULT_INCREASE_RATE = 1;
    private static final double DEFAULT_DECREASE_FACTOR = 0.5;
    private static final int DEFAULT_RATE_THRESHOLD = 10;

    @lombok.Builder.Default
    private final int maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
    @lombok.Builder.Default
    private final int initialMaxInFlightRequests = DEFAULT_INITIAL_MAX_IN_FLIGHT_REQUESTS;
    @lombok.Builder.Default
    private final int maxInFlightRequests = DEFAULT_MAX_IN_FLIGHT_REQUESTS;
    @lombok.Builder.Default
    private final int maxBufferedRequests = DEFAULT_MAX_BUFFERED_REQUESTS;
    @lombok.Builder.Default
    private final long maxBatchSizeInBytes = DEFAULT_MAX_BATCH_SIZE_IN_BYTES;
    @lombok.Builder.Default
    private final long maxTimeInBufferMS = DEFAULT_MAX_TIME_IN_BUFFER_MS;
    @lombok.Builder.Default
    private final long maxRecordSizeInBytes = DEFAULT_MAX_RECORD_SIZE_IN_BYTES;
    @lombok.Builder.Default
    private final int sinkWriterThreadPoolSize = DEFAULT_SINK_WRITER_THREAD_POOL_SIZE;
    @lombok.Builder.Default
    private final int increaseRate = DEFAULT_INCREASE_RATE;
    @lombok.Builder.Default
    private final double decreaseFactor = DEFAULT_DECREASE_FACTOR;
    @lombok.Builder.Default
    private final int rateThreshold = DEFAULT_RATE_THRESHOLD;

    /**
     * Returns a {@link SinkWriterConfig} with all default values.
     *
     * @return default config
     */
    public static SinkWriterConfig defaults() {
        return new SinkWriterConfig(
                DEFAULT_MAX_BATCH_SIZE,
                DEFAULT_INITIAL_MAX_IN_FLIGHT_REQUESTS,
                DEFAULT_MAX_IN_FLIGHT_REQUESTS,
                DEFAULT_MAX_BUFFERED_REQUESTS,
                DEFAULT_MAX_BATCH_SIZE_IN_BYTES,
                DEFAULT_MAX_TIME_IN_BUFFER_MS,
                DEFAULT_MAX_RECORD_SIZE_IN_BYTES,
                DEFAULT_SINK_WRITER_THREAD_POOL_SIZE,
                DEFAULT_INCREASE_RATE,
                DEFAULT_DECREASE_FACTOR,
                DEFAULT_RATE_THRESHOLD);
    }
}
