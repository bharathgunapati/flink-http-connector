package io.flink.connector.http.example;

import io.flink.connector.http.HttpSink;
import io.flink.connector.http.config.HttpSinkConfig;
import io.flink.connector.http.config.SinkWriterConfig;
import io.flink.connector.http.model.HttpSinkRecord;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Example demonstrating PUT requests with the Flink HTTP Sink Connector.
 *
 * <p>Sends a stream of update events via PUT to https://httpbin.org/put (echo service).
 */
public class HttpSinkPutExample {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSinkPutExample.class);
    private static final String DEFAULT_ENDPOINT = "https://httpbin.org/put";

    public static void main(String[] args) throws Exception {
        String endpointUrl = System.getProperty("http.endpoint.url", DEFAULT_ENDPOINT);
        LOG.info("Starting HttpSink PUT Example, endpoint: {}", endpointUrl);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ElementConverter<String, HttpSinkRecord> converter =
                (element, context) ->
                        HttpSinkRecord.builder()
                                .method("PUT")
                                .url(endpointUrl)
                                .headers(Map.of("Content-Type", "application/json"))
                                .body(
                                        Map.of(
                                                "id", "resource-" + (context.timestamp() != null ? context.timestamp() : System.currentTimeMillis()),
                                                "data", element,
                                                "action", "update"))
                                .build();

        HttpSinkConfig config =
                HttpSinkConfig.builder()
                        .sinkWriterConfig(
                                SinkWriterConfig.builder()
                                        .maxBatchSize(10)
                                        .maxTimeInBufferMS(1000)
                                        .build())
                        .build();

        DataStream<String> stream =
                env.fromElements(
                                "Update item A",
                                "Update item B",
                                "Update item C")
                        .map(
                                msg -> {
                                    LOG.info("PUT record: {}", msg);
                                    return msg;
                                });

        stream.sinkTo(new HttpSink<>(converter, config));
        LOG.info("Submitting job...");
        env.execute("Flink HTTP Sink PUT Example");
        LOG.info("Job completed successfully");
    }
}
