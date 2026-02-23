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
 * Example demonstrating DELETE requests with the Flink HTTP Sink Connector.
 *
 * <p>Sends a stream of delete requests to https://httpbin.org/delete (echo service).
 * DELETE typically has no body; this example includes optional metadata for demonstration.
 */
public class HttpSinkDeleteExample {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSinkDeleteExample.class);
    private static final String DEFAULT_ENDPOINT = "https://httpbin.org/delete";

    public static void main(String[] args) throws Exception {
        String endpointUrl = System.getProperty("http.endpoint.url", DEFAULT_ENDPOINT);
        LOG.info("Starting HttpSink DELETE Example, endpoint: {}", endpointUrl);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ElementConverter<String, HttpSinkRecord> converter =
                (element, context) ->
                        HttpSinkRecord.builder()
                                .method("DELETE")
                                .url(endpointUrl)
                                .headers(Map.of("Content-Type", "application/json"))
                                .body(Map.of("resource_id", element))
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
                                "resource-001",
                                "resource-002",
                                "resource-003")
                        .map(
                                msg -> {
                                    LOG.info("DELETE record: {}", msg);
                                    return msg;
                                });

        stream.sinkTo(new HttpSink<>(converter, config));
        LOG.info("Submitting job...");
        env.execute("Flink HTTP Sink DELETE Example");
        LOG.info("Job completed successfully");
    }
}
