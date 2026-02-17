package io.flink.connector.http.sink.writer;

import io.flink.connector.http.client.HttpSinkRequestSubmitter;
import io.flink.connector.http.config.HttpSinkConfig;
import io.flink.connector.http.model.HttpExchange;
import io.flink.connector.http.model.HttpSinkRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Flink sink writer that buffers {@link HttpSinkRecord} elements and submits them as HTTP requests
 * via {@link HttpSinkRequestSubmitter}.
 *
 * <p>Uses an async, non-blocking model: records are batched and sent in parallel. Failed requests
 * can be retried according to the configured retry strategy.
 *
 * @param <InputT> the type of elements from the upstream Flink job (converted to HttpSinkRecord
 *     by the element converter)
 */
@Slf4j
public class HttpSinkWriter<InputT> extends AsyncSinkWriter<InputT, HttpSinkRecord> {

    private final ExecutorService sinkWriterThreadPool;
    private final HttpSinkRequestSubmitter requestSubmitter;
    private final Counter numRecordsSendErrorsCounter;
    private final ScheduledExecutorService executorService =
            Executors.newSingleThreadScheduledExecutor();

    /**
     * Creates an HTTP sink writer.
     *
     * @param elementConverter converts upstream elements to {@link HttpSinkRecord}
     * @param context Flink sink writer init context
     * @param asyncSinkWriterConfiguration async writer configuration (batch size, etc.)
     * @param httpSinkConfig HTTP sink configuration
     * @param bufferedRequestStates recovered buffered state from checkpoint (may be empty)
     */
    public HttpSinkWriter(
            ElementConverter<InputT, HttpSinkRecord> elementConverter,
            InitContext context,
            AsyncSinkWriterConfiguration asyncSinkWriterConfiguration,
            HttpSinkConfig httpSinkConfig,
            Collection<BufferedRequestState<HttpSinkRecord>> bufferedRequestStates) {
        super(elementConverter, context, asyncSinkWriterConfiguration, bufferedRequestStates);
        SinkWriterMetricGroup metrics = context.metricGroup();
        this.requestSubmitter = new HttpSinkRequestSubmitter(httpSinkConfig, metrics);
        this.numRecordsSendErrorsCounter = metrics.getNumRecordsSendErrorsCounter();

        this.sinkWriterThreadPool =
                Executors.newFixedThreadPool(
                        httpSinkConfig.getSinkWriterThreadPoolSize(),
                        new ExecutorThreadFactory("http-sink-writer-worker"));
    }

    @Override
    protected void submitRequestEntries(
            List<HttpSinkRecord> records, ResultHandler<HttpSinkRecord> recordsToRetry) {
        if (CollectionUtils.isNotEmpty(records)) {
            CompletableFuture<List<HttpExchange>> future = requestSubmitter.submitRequests(records);
            future.whenCompleteAsync(
                    (response, err) -> handleResponse(records, recordsToRetry, response, err),
                    sinkWriterThreadPool);
        }
    }

    @Override
    protected long getSizeInBytes(HttpSinkRecord httpSinkRecord) {
        return httpSinkRecord.getSizeInBytes();
    }

    private void handleResponse(
            List<HttpSinkRecord> records,
            ResultHandler<HttpSinkRecord> recordsToRetry,
            List<HttpExchange> responses,
            Throwable err) {
        if (err != null) {
            handleErrorResponse(records.size(), err);
        } else {
            handleSuccessRecords(responses);
            recordsToRetry.retryForEntries(Collections.emptyList());
        }
    }

    private void handleSuccessRecords(List<HttpExchange> response) {
        for (HttpExchange wrapper : response) {
        }
    }

    private void handleErrorResponse(int totalRequests, Throwable err) {
        numRecordsSendErrorsCounter.inc(totalRequests);
        log.error("received exception from http-client submitRequests", err);

        getFatalExceptionCons()
                .accept(
                        new RuntimeException(
                                "received exception from http-client submitRequests", err));
    }

    private void handleFailedRecords(List<HttpExchange> response) {
        long totalRequests = response.size();
        numRecordsSendErrorsCounter.inc(totalRequests);
    }

    @Override
    public void close() {
        sinkWriterThreadPool.shutdownNow();
        requestSubmitter.close();
        super.close();
    }
}
