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
 * Example demonstrating the Flink HTTP Sink Connector.
 *
 * <p>Sends a stream of events to an HTTP endpoint. By default uses httpbin.org/post (echo service).
 * Override with system property: -Dhttp.endpoint.url=https://your-api.com/ingest
 *
 * <p>Run from the connector root: mvn exec:java -pl example
 */
public class HttpSinkExample {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSinkExample.class);
    private static final String DEFAULT_ENDPOINT = "https://httpbin.org/post";

    public static void main(String[] args) throws Exception {
        String endpointUrl =
                System.getProperty("http.endpoint.url", DEFAULT_ENDPOINT);
        LOG.info("Starting Flink HTTP Sink Example, endpoint: {}", endpointUrl);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ElementConverter<String, HttpSinkRecord> converter =
                (element, context) ->
                        HttpSinkRecord.builder()
                                .method("POST")
                                .url(endpointUrl)
                                .headers(Map.of("Content-Type", "application/json"))
                                .body(
                                        Map.of(
                                                "message", element,
                                                "timestamp", System.currentTimeMillis()))
                                .build();

        HttpSinkConfig config =
                HttpSinkConfig.builder()
                        .sinkWriterConfig(
                                SinkWriterConfig.builder()
                                        .maxBatchSize(10)
                                        .maxTimeInBufferMS(1000)
                                        .build())
                        .build();

        HttpSink<String> sink = new HttpSink<>(converter, config);

        DataStream<String> stream =
                env.fromElements(
                        "Hello Flink",
                        "HTTP Sink Example",
                        "Event streaming",
                        "At-least-once delivery")
                        .map(
                                msg -> {
                                    LOG.info("Sending record: {}", msg);
                                    return msg;
                                });

        stream.sinkTo(sink);
        LOG.info("Submitting job...");
        env.execute("Flink HTTP Sink Example");
        LOG.info("Job completed successfully");
    }
}
