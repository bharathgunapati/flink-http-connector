package io.flink.connector.http.client.apache;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.flink.connector.http.config.HttpClientConfig;
import io.flink.connector.http.config.RetryConfig;
import io.flink.connector.http.model.HttpResponse;
import io.flink.connector.http.model.HttpSinkRecord;
import org.apache.flink.metrics.MetricGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class ApacheHttpRequestExecutorTest {

    private WireMockServer wireMockServer;
    private ApacheHttpRequestExecutor executor;

    @Mock
    private MetricGroup metricGroup;

    @BeforeEach
    void setUp() {
        wireMockServer = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        wireMockServer.start();
    }

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.close();
        }
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    private ApacheHttpRequestExecutor createExecutor() {
        return new ApacheHttpRequestExecutor(
                metricGroup, HttpClientConfig.defaults(), RetryConfig.defaults());
    }

    @Test
    void execute_invalidHttpMethod_throwsIllegalArgumentException() {
        executor = createExecutor();

        HttpSinkRecord record = HttpSinkRecord.builder()
                .method("INVALID")
                .url("http://localhost:8080/test")
                .headers(Map.of())
                .body(Map.of())
                .build();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> executor.execute(record));
        assertTrue(ex.getMessage().contains("Invalid HTTP method"));
    }

    @Test
    void execute_urlWithCurlyBraces_encodesAndSendsRequest() throws Exception {
        int port = wireMockServer.port();
        wireMockServer.stubFor(
                get(urlPathEqualTo("/resource/%7Bid%7D"))
                        .willReturn(aResponse().withStatus(200).withBody("ok")));

        executor = createExecutor();

        // URL with curly braces - executor encodes them to %7B and %7D
        HttpSinkRecord record = HttpSinkRecord.builder()
                .method("GET")
                .url("http://localhost:" + port + "/resource/{id}")
                .headers(Map.of())
                .body(null)
                .build();

        CompletableFuture<HttpResponse> future = executor.execute(record);
        HttpResponse response = future.get(5, TimeUnit.SECONDS);

        assertEquals(200, response.getStatusCode());
        assertEquals("ok", response.getBody());
        wireMockServer.verify(getRequestedFor(urlPathEqualTo("/resource/%7Bid%7D")));
    }

    @Test
    void execute_customContentType_sendsCorrectHeader() throws Exception {
        int port = wireMockServer.port();
        wireMockServer.stubFor(
                post(urlPathEqualTo("/ingest"))
                        .withHeader("Content-Type", containing("application/xml"))
                        .willReturn(aResponse().withStatus(200).withBody("ok")));

        executor = createExecutor();

        HttpSinkRecord record = HttpSinkRecord.builder()
                .method("POST")
                .url("http://localhost:" + port + "/ingest")
                .headers(Map.of("Content-Type", "application/xml"))
                .body(Map.of("data", "value"))
                .build();

        CompletableFuture<HttpResponse> future = executor.execute(record);
        HttpResponse response = future.get(5, TimeUnit.SECONDS);

        assertEquals(200, response.getStatusCode());
        wireMockServer.verify(postRequestedFor(urlPathEqualTo("/ingest"))
                .withHeader("Content-Type", containing("application/xml")));
    }

    @Test
    void execute_allSupportedMethods_succeed() throws Exception {
        int port = wireMockServer.port();
        wireMockServer.stubFor(get(urlPathEqualTo("/get")).willReturn(aResponse().withStatus(200)));
        wireMockServer.stubFor(post(urlPathEqualTo("/post")).willReturn(aResponse().withStatus(200)));
        wireMockServer.stubFor(put(urlPathEqualTo("/put")).willReturn(aResponse().withStatus(200)));
        wireMockServer.stubFor(patch(urlPathEqualTo("/patch")).willReturn(aResponse().withStatus(200)));
        wireMockServer.stubFor(delete(urlPathEqualTo("/delete")).willReturn(aResponse().withStatus(200)));
        wireMockServer.stubFor(head(urlPathEqualTo("/head")).willReturn(aResponse().withStatus(200)));

        executor = createExecutor();
        String baseUrl = "http://localhost:" + port;

        for (String method : new String[]{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"}) {
            HttpSinkRecord record = HttpSinkRecord.builder()
                    .method(method)
                    .url(baseUrl + "/" + method.toLowerCase())
                    .headers(Map.of("Content-Type", "application/json"))
                    .body(method.equals("GET") || method.equals("DELETE") || method.equals("HEAD")
                            ? Map.of() : Map.of("k", "v"))
                    .build();

            HttpResponse response = executor.execute(record).get(5, TimeUnit.SECONDS);
            assertEquals(200, response.getStatusCode(), "Failed for method " + method);
        }
    }

    @Test
    void close_doesNotThrow() {
        executor = createExecutor();
        assertDoesNotThrow(executor::close);
    }
}
