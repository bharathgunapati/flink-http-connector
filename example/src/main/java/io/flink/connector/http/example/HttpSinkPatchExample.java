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
 * Example demonstrating PATCH requests with the Flink HTTP Sink Connector.
 *
 * <p>Sends a stream of partial update events via PATCH to https://httpbin.org/patch (echo service).
 */
public class HttpSinkPatchExample {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSinkPatchExample.class);
    private static final String DEFAULT_ENDPOINT = "https://httpbin.org/patch";

    public static void main(String[] args) throws Exception {
        String endpointUrl = System.getProperty("http.endpoint.url", DEFAULT_ENDPOINT);
        LOG.info("Starting HttpSink PATCH Example, endpoint: {}", endpointUrl);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ElementConverter<String, HttpSinkRecord> converter =
                (element, context) ->
                        HttpSinkRecord.builder()
                                .method("PATCH")
                                .url(endpointUrl)
                                .headers(Map.of("Content-Type", "application/json"))
                                .body(
                                        Map.of(
                                                "patch_field", element,
                                                "partial_update", true))
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
                                "Partial update 1",
                                "Partial update 2",
                                "Partial update 3")
                        .map(
                                msg -> {
                                    LOG.info("PATCH record: {}", msg);
                                    return msg;
                                });

        stream.sinkTo(new HttpSink<>(converter, config));
        LOG.info("Submitting job...");
        env.execute("Flink HTTP Sink PATCH Example");
        LOG.info("Job completed successfully");
    }
}
