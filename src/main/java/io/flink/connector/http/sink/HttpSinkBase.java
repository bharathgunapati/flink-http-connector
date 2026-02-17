package io.flink.connector.http.sink;

import io.flink.connector.http.config.HttpSinkConfig;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import io.flink.connector.http.model.HttpSinkRecord;
import io.flink.connector.http.sink.writer.HttpSinkWriter;
import io.flink.connector.http.sink.writer.HttpSinkWriterStateSerializer;
import org.apache.flink.connector.base.sink.AsyncSinkBase;

/**
 * Base class for the HTTP sink that extends Flink's {@link AsyncSinkBase} to provide async,
 * buffered HTTP request writing with configurable batching and rate limiting.
 *
 * <p>Delegates to {@link HttpSinkWriter} for the actual writing logic and uses
 * {@link HttpSinkConfig} for all configuration (sink writer, HTTP client, and retry settings).
 *
 * @param <InputT> the type of elements from the upstream Flink job
 */
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.base.sink.writer.strategy.AIMDScalingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.CongestionControlRateLimitingStrategy;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class HttpSinkBase<InputT> extends AsyncSinkBase<InputT, HttpSinkRecord> {

  private final HttpSinkConfig httpSinkConfig;

  /**
   * Creates an HTTP sink base with the given element converter and configuration.
   *
   * @param elementConverter converts upstream elements to {@link HttpSinkRecord}
   * @param httpSinkConfig sink, HTTP client, and retry configuration
   */
  public HttpSinkBase(
      ElementConverter<InputT, HttpSinkRecord> elementConverter,
      HttpSinkConfig httpSinkConfig) {
    super(
        elementConverter,
        httpSinkConfig.getMaxBatchSize(),
        httpSinkConfig.getMaxInFlightRequests(),
        httpSinkConfig.getMaxBufferedRequests(),
        httpSinkConfig.getMaxBatchSizeInBytes(),
        httpSinkConfig.getMaxTimeInBufferMS(),
        httpSinkConfig.getMaxRecordSizeInBytes());

    this.httpSinkConfig = httpSinkConfig;
  }

  @Override
  public StatefulSinkWriter<InputT, BufferedRequestState<HttpSinkRecord>> createWriter(
      InitContext context) throws IOException {
    return createHttpSinkWriter(context);
  }

  @Override
  public StatefulSinkWriter<InputT, BufferedRequestState<HttpSinkRecord>> restoreWriter(
      InitContext context, Collection<BufferedRequestState<HttpSinkRecord>> recoveredState)
      throws IOException {
    return createHttpSinkWriter(context);
  }

  private HttpSinkWriter<InputT> createHttpSinkWriter(InitContext context) {
    AIMDScalingStrategy aimdScalingStrategy =
        AIMDScalingStrategy.builder(httpSinkConfig.getRateThreshold())
            .setIncreaseRate(httpSinkConfig.getIncreaseRate())
            .setDecreaseFactor(httpSinkConfig.getDecreaseFactor())
            .build();

    CongestionControlRateLimitingStrategy congestionControlRateLimitingStrategy =
        CongestionControlRateLimitingStrategy.builder()
            .setInitialMaxInFlightMessages(httpSinkConfig.getInitialMaxInFlightRequests())
            .setMaxInFlightRequests(httpSinkConfig.getMaxInFlightRequests())
            .setScalingStrategy(aimdScalingStrategy)
            .build();

    AsyncSinkWriterConfiguration asyncSinkWriterConfiguration =
        new AsyncSinkWriterConfiguration.AsyncSinkWriterConfigurationBuilder()
            .setMaxBatchSize(getMaxBatchSize())
            .setMaxBatchSizeInBytes(getMaxBatchSizeInBytes())
            .setMaxInFlightRequests(getMaxInFlightRequests())
            .setMaxBufferedRequests(getMaxBufferedRequests())
            .setMaxTimeInBufferMS(getMaxTimeInBufferMS())
            .setMaxRecordSizeInBytes(getMaxRecordSizeInBytes())
            .setRateLimitingStrategy(congestionControlRateLimitingStrategy)
            .build();

    return new HttpSinkWriter<>(
        getElementConverter(),
        context,
        asyncSinkWriterConfiguration,
            httpSinkConfig,
        Collections.emptyList());
  }

  @Override
  public SimpleVersionedSerializer<BufferedRequestState<HttpSinkRecord>> getWriterStateSerializer() {
    return new HttpSinkWriterStateSerializer();
  }
}
