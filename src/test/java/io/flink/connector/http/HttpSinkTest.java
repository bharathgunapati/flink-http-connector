package io.flink.connector.http;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.flink.connector.http.config.HttpSinkConfig;
import io.flink.connector.http.config.RetryConfig;
import io.flink.connector.http.config.SinkWriterConfig;
import io.flink.connector.http.model.HttpSinkRecord;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

/**
 * Integration test for {@link HttpSink}. Uses Flink's {@link AbstractTestBase} (MiniCluster) to run
 * jobs that send records to a mock HTTP server (WireMock).
 */
class HttpSinkTest extends AbstractTestBase {

    private static WireMockServer wireMockServer;

    @BeforeAll
    static void setUp() {
        wireMockServer = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        wireMockServer.start();
    }

    @AfterAll
    static void tearDown() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @BeforeEach
    void resetWireMock() {
        wireMockServer.resetAll();
    }

    @Test
    void httpSink_sendsRecordsToEndpoint() throws Exception {
        int port = wireMockServer.port();
        String baseUrl = "http://localhost:" + port;

        wireMockServer.stubFor(
                post(urlPathEqualTo("/ingest"))
                        .willReturn(aResponse().withStatus(200).withBody("ok")));

        ElementConverter<String, HttpSinkRecord> elementConverter =
                (element, context) ->
                        HttpSinkRecord.builder()
                                .method("POST")
                                .url(baseUrl + "/ingest")
                                .headers(Map.of("Content-Type", "application/json"))
                                .body(Map.of("payload", element))
                                .build();

        HttpSinkConfig config =
                HttpSinkConfig.builder()
                        .sinkWriterConfig(
                                SinkWriterConfig.builder()
                                        .maxBatchSize(10)
                                        .maxTimeInBufferMS(500)
                                        .build())
                        .build();

        HttpSink<String> sink = new HttpSink<>(elementConverter, config);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> stream = env.fromElements("a", "b", "c");
        stream.sinkTo(sink);

        env.execute();

        wireMockServer.verify(3, postRequestedFor(urlPathEqualTo("/ingest")));
    }

    @Test
    void httpSink_sendsCorrectRequestBody() throws Exception {
        int port = wireMockServer.port();
        String baseUrl = "http://localhost:" + port;

        wireMockServer.stubFor(
                post(urlPathEqualTo("/events"))
                        .willReturn(aResponse().withStatus(201).withBody("created")));

        ElementConverter<String, HttpSinkRecord> elementConverter =
                (element, context) ->
                        HttpSinkRecord.builder()
                                .method("POST")
                                .url(baseUrl + "/events")
                                .headers(Map.of("Content-Type", "application/json"))
                                .body(Map.of("event", element, "timestamp", System.currentTimeMillis()))
                                .build();

        HttpSinkConfig config =
                HttpSinkConfig.builder()
                        .sinkWriterConfig(
                                SinkWriterConfig.builder()
                                        .maxBatchSize(5)
                                        .maxTimeInBufferMS(500)
                                        .build())
                        .build();

        HttpSink<String> sink = new HttpSink<>(elementConverter, config);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements("event-1").sinkTo(sink);
        env.execute();

        wireMockServer.verify(
                postRequestedFor(urlPathEqualTo("/events"))
                        .withRequestBody(containing("\"event\":\"event-1\"")));
    }

    @Test
    void httpSink_serverReturns500_requestStillSent() throws Exception {
        int port = wireMockServer.port();
        String baseUrl = "http://localhost:" + port;

        wireMockServer.stubFor(
                post(urlPathEqualTo("/error"))
                        .willReturn(aResponse().withStatus(500).withBody("internal error")));

        ElementConverter<String, HttpSinkRecord> elementConverter =
                (element, context) ->
                        HttpSinkRecord.builder()
                                .method("POST")
                                .url(baseUrl + "/error")
                                .headers(Map.of("Content-Type", "application/json"))
                                .body(Map.of("data", element))
                                .build();

        HttpSinkConfig config =
                HttpSinkConfig.builder()
                        .sinkWriterConfig(
                                SinkWriterConfig.builder()
                                        .maxBatchSize(5)
                                        .maxTimeInBufferMS(500)
                                        .build())
                        .build();

        HttpSink<String> sink = new HttpSink<>(elementConverter, config);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements("x").sinkTo(sink);
        env.execute();

        wireMockServer.verify(postRequestedFor(urlPathEqualTo("/error")));
    }

    @Test
    void httpSink_serverReturns400_requestStillSent() throws Exception {
        int port = wireMockServer.port();
        String baseUrl = "http://localhost:" + port;

        wireMockServer.stubFor(
                post(urlPathEqualTo("/bad-request"))
                        .willReturn(aResponse().withStatus(400).withBody("bad request")));

        ElementConverter<String, HttpSinkRecord> elementConverter =
                (element, context) ->
                        HttpSinkRecord.builder()
                                .method("POST")
                                .url(baseUrl + "/bad-request")
                                .headers(Map.of("Content-Type", "application/json"))
                                .body(Map.of("payload", element))
                                .build();

        HttpSinkConfig config =
                HttpSinkConfig.builder()
                        .sinkWriterConfig(
                                SinkWriterConfig.builder()
                                        .maxBatchSize(5)
                                        .maxTimeInBufferMS(500)
                                        .build())
                        .build();

        HttpSink<String> sink = new HttpSink<>(elementConverter, config);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements("invalid-data").sinkTo(sink);
        env.execute();

        wireMockServer.verify(1, postRequestedFor(urlPathEqualTo("/bad-request")));
    }

    @Test
    void httpSink_serverReturns429_requestStillSent() throws Exception {
        int port = wireMockServer.port();
        String baseUrl = "http://localhost:" + port;

        wireMockServer.stubFor(
                post(urlPathEqualTo("/rate-limit"))
                        .willReturn(
                                aResponse()
                                        .withStatus(429)
                                        .withHeader("Retry-After", "1")
                                        .withBody("too many requests")));

        ElementConverter<String, HttpSinkRecord> elementConverter =
                (element, context) ->
                        HttpSinkRecord.builder()
                                .method("POST")
                                .url(baseUrl + "/rate-limit")
                                .headers(Map.of("Content-Type", "application/json"))
                                .body(Map.of("id", element))
                                .build();

        HttpSinkConfig config =
                HttpSinkConfig.builder()
                        .sinkWriterConfig(
                                SinkWriterConfig.builder()
                                        .maxBatchSize(5)
                                        .maxTimeInBufferMS(500)
                                        .build())
                        .build();

        HttpSink<String> sink = new HttpSink<>(elementConverter, config);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements("req-1").sinkTo(sink);
        env.execute();

        wireMockServer.verify(postRequestedFor(urlPathEqualTo("/rate-limit")));
    }

    @Test
    void httpSink_noRequestsWhenStreamEmpty() throws Exception {
        int port = wireMockServer.port();
        String baseUrl = "http://localhost:" + port;

        wireMockServer.stubFor(
                post(urlPathEqualTo("/ingest"))
                        .willReturn(aResponse().withStatus(200).withBody("ok")));

        ElementConverter<String, HttpSinkRecord> elementConverter =
                (element, context) ->
                        HttpSinkRecord.builder()
                                .method("POST")
                                .url(baseUrl + "/ingest")
                                .headers(Map.of("Content-Type", "application/json"))
                                .body(Map.of("payload", element))
                                .build();

        HttpSinkConfig config =
                HttpSinkConfig.builder()
                        .sinkWriterConfig(
                                SinkWriterConfig.builder()
                                        .maxBatchSize(10)
                                        .maxTimeInBufferMS(500)
                                        .build())
                        .build();

        HttpSink<String> sink = new HttpSink<>(elementConverter, config);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> emptyStream =
                env.fromCollection(Collections.emptyList(), Types.STRING);
        emptyStream.sinkTo(sink);

        env.execute();

        wireMockServer.verify(0, postRequestedFor(urlPathEqualTo("/ingest")));
    }

    @Test
    void httpSink_retriesOn500_untilSuccess() throws Exception {
        int port = wireMockServer.port();
        String baseUrl = "http://localhost:" + port;

        wireMockServer.stubFor(
                post(urlPathEqualTo("/retry-endpoint"))
                        .inScenario("Retry scenario")
                        .whenScenarioStateIs("Started")
                        .willReturn(aResponse().withStatus(500).withBody("error"))
                        .willSetStateTo("First retry"));
        wireMockServer.stubFor(
                post(urlPathEqualTo("/retry-endpoint"))
                        .inScenario("Retry scenario")
                        .whenScenarioStateIs("First retry")
                        .willReturn(aResponse().withStatus(500).withBody("error"))
                        .willSetStateTo("Second retry"));
        wireMockServer.stubFor(
                post(urlPathEqualTo("/retry-endpoint"))
                        .inScenario("Retry scenario")
                        .whenScenarioStateIs("Second retry")
                        .willReturn(aResponse().withStatus(200).withBody("ok")));

        ElementConverter<String, HttpSinkRecord> elementConverter =
                (element, context) ->
                        HttpSinkRecord.builder()
                                .method("POST")
                                .url(baseUrl + "/retry-endpoint")
                                .headers(Map.of("Content-Type", "application/json"))
                                .body(Map.of("data", element))
                                .build();

        HttpSinkConfig config =
                HttpSinkConfig.builder()
                        .sinkWriterConfig(
                                SinkWriterConfig.builder()
                                        .maxBatchSize(1)
                                        .maxTimeInBufferMS(100)
                                        .build())
                        .retryConfig(
                                RetryConfig.builder()
                                        .retryEnabled(true)
                                        .maxRetries(3)
                                        .delayInSecs(0L)
                                        .maxDelayInSecs(0L)
                                        .transientStatusCodes(List.of(500, 502, 503))
                                        .nonRetryableStatusCodes(List.of(400))
                                        .build())
                        .build();

        HttpSink<String> sink = new HttpSink<>(elementConverter, config);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements("x").sinkTo(sink);
        env.execute();

        wireMockServer.verify(3, postRequestedFor(urlPathEqualTo("/retry-endpoint")));
    }

    @Test
    void httpSink_retriesDisabled_onlyOneRequestOn500() throws Exception {
        int port = wireMockServer.port();
        String baseUrl = "http://localhost:" + port;

        wireMockServer.stubFor(
                post(urlPathEqualTo("/no-retry"))
                        .willReturn(aResponse().withStatus(500).withBody("internal error")));

        ElementConverter<String, HttpSinkRecord> elementConverter =
                (element, context) ->
                        HttpSinkRecord.builder()
                                .method("POST")
                                .url(baseUrl + "/no-retry")
                                .headers(Map.of("Content-Type", "application/json"))
                                .body(Map.of("data", element))
                                .build();

        HttpSinkConfig config =
                HttpSinkConfig.builder()
                        .sinkWriterConfig(
                                SinkWriterConfig.builder()
                                        .maxBatchSize(1)
                                        .maxTimeInBufferMS(100)
                                        .build())
                        .retryConfig(
                                RetryConfig.builder()
                                        .retryEnabled(false)
                                        .maxRetries(0)
                                        .build())
                        .build();

        HttpSink<String> sink = new HttpSink<>(elementConverter, config);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements("x").sinkTo(sink);
        env.execute();

        wireMockServer.verify(1, postRequestedFor(urlPathEqualTo("/no-retry")));
    }

}
