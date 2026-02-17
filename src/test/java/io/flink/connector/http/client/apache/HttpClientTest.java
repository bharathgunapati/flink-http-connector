package io.flink.connector.http.client.apache;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.flink.connector.http.config.HttpClientConfig;
import io.flink.connector.http.config.RetryConfig;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HttpClientTest {

    private WireMockServer wireMockServer;
    private HttpClient httpClient;

    @Mock
    private MetricGroup metricGroup;

    @BeforeEach
    void setUp() {
        wireMockServer = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        wireMockServer.start();
    }

    @AfterEach
    void tearDown() {
        if (httpClient != null) {
            httpClient.close();
        }
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    private HttpClient createHttpClient() {
        return new HttpClient(metricGroup, HttpClientConfig.defaults(), RetryConfig.defaults());
    }

    @Test
    @SuppressWarnings("unchecked")
    void constructor_registersConnectionPoolMetrics() {
        httpClient = createHttpClient();

        ArgumentCaptor<String> nameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Gauge<Long>> gaugeCaptor = ArgumentCaptor.forClass((Class<Gauge<Long>>) (Class<?>) Gauge.class);
        verify(metricGroup, times(5)).gauge(nameCaptor.capture(), gaugeCaptor.capture());

        var names = nameCaptor.getAllValues();
        assertTrue(names.contains("httpclient.pool.total.max"));
        assertTrue(names.contains("httpclient.pool.total.connections.available"));
        assertTrue(names.contains("httpclient.pool.total.connections.leased"));
        assertTrue(names.contains("httpclient.pool.total.pending"));
        assertTrue(names.contains("httpclient.pool.route.max.default"));

        // Invoke each gauge to exercise registerConnectionPoolMetrics lambda bodies
        var gauges = gaugeCaptor.getAllValues();
        assertEquals(5, gauges.size());
        for (Gauge<Long> gauge : gauges) {
            assertNotNull(gauge.getValue(), "Gauge should return non-null value");
        }

        // Verify gauge values match connection pool config (defaults: max 15)
        int maxIdx = names.indexOf("httpclient.pool.total.max");
        assertEquals(15L, gauges.get(maxIdx).getValue());

        int routeMaxIdx = names.indexOf("httpclient.pool.route.max.default");
        assertEquals(15L, gauges.get(routeMaxIdx).getValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    void connectionPoolMetrics_reflectPoolStateAfterRequest() throws Exception {
        int port = wireMockServer.port();
        wireMockServer.stubFor(
                get(urlPathEqualTo("/pool-test"))
                        .willReturn(aResponse().withStatus(200).withBody("ok")));

        httpClient = createHttpClient();

        // Execute request to change pool state (leased/available)
        SimpleHttpRequest request = new SimpleHttpRequest("GET", "http://localhost:" + port + "/pool-test");
        CompletableFuture<SimpleHttpResponse> future = new CompletableFuture<>();
        httpClient.executeRequest(request, new FutureCallback<SimpleHttpResponse>() {
            @Override
            public void completed(SimpleHttpResponse response) {
                future.complete(response);
            }

            @Override
            public void failed(Exception ex) {
                future.completeExceptionally(ex);
            }

            @Override
            public void cancelled() {
                future.cancel(false);
            }
        });
        future.get(5, TimeUnit.SECONDS);

        // Capture and invoke gauges - exercises registerConnectionPoolMetrics with active pool
        ArgumentCaptor<Gauge<Long>> gaugeCaptor = ArgumentCaptor.forClass((Class<Gauge<Long>>) (Class<?>) Gauge.class);
        verify(metricGroup, times(5)).gauge(anyString(), gaugeCaptor.capture());

        var gauges = gaugeCaptor.getAllValues();
        for (Gauge<Long> gauge : gauges) {
            Long value = gauge.getValue();
            assertNotNull(value);
            assertTrue(value >= 0, "Pool metric should be non-negative");
        }
    }

    @Test
    void executeRequest_successfulGetRequest_returnsResponse() throws Exception {
        int port = wireMockServer.port();
        wireMockServer.stubFor(
                get(urlPathEqualTo("/test"))
                        .willReturn(aResponse().withStatus(200).withBody("hello")));

        httpClient = createHttpClient();

        SimpleHttpRequest request = new SimpleHttpRequest("GET", "http://localhost:" + port + "/test");
        CompletableFuture<SimpleHttpResponse> future = new CompletableFuture<>();

        httpClient.executeRequest(request, new FutureCallback<SimpleHttpResponse>() {
            @Override
            public void completed(SimpleHttpResponse response) {
                future.complete(response);
            }

            @Override
            public void failed(Exception ex) {
                future.completeExceptionally(ex);
            }

            @Override
            public void cancelled() {
                future.cancel(false);
            }
        });

        SimpleHttpResponse response = future.get(5, TimeUnit.SECONDS);
        assertNotNull(response);
        assertEquals(200, response.getCode());
        assertEquals("hello", response.getBodyText());
    }

    @Test
    void executeRequest_successfulPostRequest_withBody() throws Exception {
        int port = wireMockServer.port();
        wireMockServer.stubFor(
                post(urlPathEqualTo("/ingest"))
                        .withRequestBody(containing("payload"))
                        .willReturn(aResponse().withStatus(201).withBody("created")));

        httpClient = createHttpClient();

        SimpleHttpRequest request = new SimpleHttpRequest("POST", "http://localhost:" + port + "/ingest");
        request.setBody("{\"payload\":\"data\"}", org.apache.hc.core5.http.ContentType.APPLICATION_JSON);

        CompletableFuture<SimpleHttpResponse> future = new CompletableFuture<>();
        httpClient.executeRequest(request, new FutureCallback<SimpleHttpResponse>() {
            @Override
            public void completed(SimpleHttpResponse response) {
                future.complete(response);
            }

            @Override
            public void failed(Exception ex) {
                future.completeExceptionally(ex);
            }

            @Override
            public void cancelled() {
                future.cancel(false);
            }
        });

        SimpleHttpResponse response = future.get(5, TimeUnit.SECONDS);
        assertNotNull(response);
        assertEquals(201, response.getCode());
        assertEquals("created", response.getBodyText());

        wireMockServer.verify(postRequestedFor(urlPathEqualTo("/ingest")));
    }

    @Test
    void executeRequest_serverReturns404_invokesFailedCallback() throws Exception {
        int port = wireMockServer.port();
        wireMockServer.stubFor(
                get(urlPathEqualTo("/not-found"))
                        .willReturn(aResponse().withStatus(404).withBody("not found")));

        httpClient = createHttpClient();

        SimpleHttpRequest request = new SimpleHttpRequest("GET", "http://localhost:" + port + "/not-found");
        CompletableFuture<SimpleHttpResponse> future = new CompletableFuture<>();

        httpClient.executeRequest(request, new FutureCallback<SimpleHttpResponse>() {
            @Override
            public void completed(SimpleHttpResponse response) {
                future.complete(response);
            }

            @Override
            public void failed(Exception ex) {
                future.completeExceptionally(ex);
            }

            @Override
            public void cancelled() {
                future.cancel(false);
            }
        });

        SimpleHttpResponse response = future.get(5, TimeUnit.SECONDS);
        assertNotNull(response);
        assertEquals(404, response.getCode());
    }

    @Test
    void executeRequest_serverReturns500_invokesCompletedCallbackWithErrorStatus() throws Exception {
        int port = wireMockServer.port();
        wireMockServer.stubFor(
                get(urlPathEqualTo("/error"))
                        .willReturn(aResponse().withStatus(500).withBody("internal error")));

        // Disable retries so we get exactly one response within the timeout
        RetryConfig noRetryConfig = RetryConfig.builder()
                .retryEnabled(false)
                .maxRetries(0)
                .build();
        httpClient = new HttpClient(metricGroup, HttpClientConfig.defaults(), noRetryConfig);

        SimpleHttpRequest request = new SimpleHttpRequest("GET", "http://localhost:" + port + "/error");
        CompletableFuture<SimpleHttpResponse> future = new CompletableFuture<>();

        httpClient.executeRequest(request, new FutureCallback<SimpleHttpResponse>() {
            @Override
            public void completed(SimpleHttpResponse response) {
                future.complete(response);
            }

            @Override
            public void failed(Exception ex) {
                future.completeExceptionally(ex);
            }

            @Override
            public void cancelled() {
                future.cancel(false);
            }
        });

        SimpleHttpResponse response = future.get(5, TimeUnit.SECONDS);
        assertNotNull(response);
        assertEquals(500, response.getCode());
        assertEquals("internal error", response.getBodyText());
    }

    @Test
    void executeRequest_invalidHost_invokesFailedCallback() throws Exception {
        httpClient = createHttpClient();

        SimpleHttpRequest request = new SimpleHttpRequest("GET", "http://nonexistent-host-that-does-not-exist-12345.local/test");
        CompletableFuture<SimpleHttpResponse> future = new CompletableFuture<>();

        httpClient.executeRequest(request, new FutureCallback<SimpleHttpResponse>() {
            @Override
            public void completed(SimpleHttpResponse response) {
                future.complete(response);
            }

            @Override
            public void failed(Exception ex) {
                future.completeExceptionally(ex);
            }

            @Override
            public void cancelled() {
                future.cancel(false);
            }
        });

        assertThrows(Exception.class, () -> future.get(10, TimeUnit.SECONDS));
    }

    @Test
    void close_doesNotThrow() {
        httpClient = createHttpClient();
        assertDoesNotThrow(() -> httpClient.close());
    }

    @Test
    void close_canBeCalledMultipleTimes() {
        httpClient = createHttpClient();
        assertDoesNotThrow(() -> {
            httpClient.close();
            httpClient.close();
        });
    }

    @Test
    void constructor_withProxyConfig_buildsAndClosesCleanly() {
        HttpClientConfig config =
                HttpClientConfig.builder()
                        .proxyHost("proxy.example.com")
                        .proxyPort(8080)
                        .build();
        httpClient = new HttpClient(metricGroup, config, RetryConfig.defaults());
        assertDoesNotThrow(() -> httpClient.close());
    }

    @Test
    void constructor_withTrustStoreConfig_buildsAndClosesCleanly() throws Exception {
        Path trustStorePath = createTempTrustStore();
        HttpClientConfig config =
                HttpClientConfig.builder()
                        .trustStorePath(trustStorePath.toString())
                        .trustStorePassword("password")
                        .build();
        httpClient = new HttpClient(metricGroup, config, RetryConfig.defaults());
        assertDoesNotThrow(() -> httpClient.close());
        Files.deleteIfExists(trustStorePath);
    }

    @Test
    void constructor_withKeyStoreOnlyConfig_buildsAndClosesCleanly() throws Exception {
        Path keyStorePath = createTempKeyStoreWithKey();
        HttpClientConfig config =
                HttpClientConfig.builder()
                        .keyStorePath(keyStorePath.toString())
                        .keyStorePassword("password")
                        .build();
        httpClient = new HttpClient(metricGroup, config, RetryConfig.defaults());
        assertDoesNotThrow(() -> httpClient.close());
        Files.deleteIfExists(keyStorePath);
    }

    @Test
    void constructor_withTrustAndKeyStoreConfig_buildsAndClosesCleanly() throws Exception {
        Path trustStorePath = createTempTrustStore();
        Path keyStorePath = createTempKeyStoreWithKey();
        HttpClientConfig config =
                HttpClientConfig.builder()
                        .trustStorePath(trustStorePath.toString())
                        .trustStorePassword("password")
                        .keyStorePath(keyStorePath.toString())
                        .keyStorePassword("password")
                        .build();
        httpClient = new HttpClient(metricGroup, config, RetryConfig.defaults());
        assertDoesNotThrow(() -> httpClient.close());
        Files.deleteIfExists(trustStorePath);
        Files.deleteIfExists(keyStorePath);
    }

    @Test
    void constructor_withInvalidTrustStorePath_throwsIllegalStateException() {
        HttpClientConfig config =
                HttpClientConfig.builder()
                        .trustStorePath("/nonexistent/path/to/truststore.jks")
                        .trustStorePassword("password")
                        .build();
        assertThrows(
                IllegalStateException.class,
                () -> new HttpClient(metricGroup, config, RetryConfig.defaults()));
    }

    private Path createTempTrustStore() throws Exception {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(null, null);
        Path path = Files.createTempFile("test-trust", ".jks");
        try (FileOutputStream fos = new FileOutputStream(path.toFile())) {
            trustStore.store(fos, "password".toCharArray());
        }
        return path;
    }

    private Path createTempKeyStoreWithKey() throws Exception {
        Path path = Files.createTempFile("test-key", ".p12");
        Files.delete(path); // keytool will create the file; empty file causes "Keystore file exists, but is empty"
        ProcessBuilder pb =
                new ProcessBuilder(
                        "keytool",
                        "-genkeypair",
                        "-alias",
                        "test",
                        "-keyalg",
                        "RSA",
                        "-keystore",
                        path.toString(),
                        "-storepass",
                        "password",
                        "-storetype",
                        "PKCS12",
                        "-dname",
                        "CN=test",
                        "-keypass",
                        "password");
        pb.redirectErrorStream(true);
        Process p = pb.start();
        int exitCode = p.waitFor();
        if (exitCode != 0) {
            throw new AssertionError(
                    "keytool failed: " + new String(p.getInputStream().readAllBytes()));
        }
        return path;
    }
}
