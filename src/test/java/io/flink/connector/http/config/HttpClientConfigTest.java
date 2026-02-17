package io.flink.connector.http.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HttpClientConfigTest {

    @Test
    void defaults_returnsConfigWithExpectedValues() {
        HttpClientConfig config = HttpClientConfig.defaults();

        assertEquals(30000, config.getConnectTimeoutMs());
        assertEquals(30000, config.getResponseTimeoutMs());
        assertEquals(15, config.getMaxConnections());
        assertEquals(15, config.getMaxConnectionsPerHost());
        assertEquals(64, config.getHttpClientThreadPoolSize());
    }

    @Test
    void getDefaultHeaders_returnsContentTypeAndAccept() {
        HttpClientConfig config = HttpClientConfig.defaults();

        Map<String, Object> headers = config.getDefaultHeaders();
        assertEquals("application/json", headers.get("Content-Type"));
        assertEquals("application/json", headers.get("Accept"));
    }

    @Test
    void builder_createsConfigWithCustomValues() {
        HttpClientConfig config = HttpClientConfig.builder()
                .connectTimeoutMs(5000)
                .maxConnections(50)
                .build();

        assertEquals(5000, config.getConnectTimeoutMs());
        assertEquals(50, config.getMaxConnections());
    }
}
