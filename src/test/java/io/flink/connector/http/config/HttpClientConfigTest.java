package io.flink.connector.http.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HttpClientConfigTest {

    @Test
    void defaults_returnsConfigWithExpectedValues() {
        HttpClientConfig config = HttpClientConfig.defaults();

        assertEquals(30000, config.getConnectTimeoutMs());
        assertEquals(30000, config.getRequestTimeoutMs());
        assertEquals(30000, config.getResponseTimeoutMs());
        assertEquals(30000, config.getSocketTimeoutMs());
        assertEquals(30000, config.getReadTimeoutMs());
        assertEquals(30000, config.getConnectionTtlMs());
        assertEquals(300_000, config.getEvictIdleConnectionTimeoutMs());
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
    void getDefaultHeaders_returnsEmptyMap_whenNull() {
        HttpClientConfig config = HttpClientConfig.builder()
                .connectTimeoutMs(30000)
                .requestTimeoutMs(30000)
                .responseTimeoutMs(30000)
                .socketTimeoutMs(30000)
                .readTimeoutMs(30000)
                .connectionTtlMs(30000)
                .evictIdleConnectionTimeoutMs(300_000)
                .maxConnections(15)
                .maxConnectionsPerHost(15)
                .httpClientThreadPoolSize(64)
                .defaultHeaders(null)
                .build();

        assertTrue(config.getDefaultHeaders().isEmpty());
    }

    @Test
    void builder_createsConfigWithCustomValues() {
        HttpClientConfig config = HttpClientConfig.builder()
                .connectTimeoutMs(5000)
                .requestTimeoutMs(10000)
                .responseTimeoutMs(15000)
                .socketTimeoutMs(8000)
                .readTimeoutMs(12000)
                .connectionTtlMs(60000)
                .evictIdleConnectionTimeoutMs(600_000)
                .maxConnections(50)
                .maxConnectionsPerHost(25)
                .httpClientThreadPoolSize(32)
                .defaultHeaders(Map.of("X-Custom", "value"))
                .build();

        assertEquals(5000, config.getConnectTimeoutMs());
        assertEquals(10000, config.getRequestTimeoutMs());
        assertEquals(15000, config.getResponseTimeoutMs());
        assertEquals(8000, config.getSocketTimeoutMs());
        assertEquals(12000, config.getReadTimeoutMs());
        assertEquals(60000, config.getConnectionTtlMs());
        assertEquals(600_000, config.getEvictIdleConnectionTimeoutMs());
        assertEquals(50, config.getMaxConnections());
        assertEquals(25, config.getMaxConnectionsPerHost());
        assertEquals(32, config.getHttpClientThreadPoolSize());
        assertEquals("value", config.getDefaultHeaders().get("X-Custom"));
    }

    @Test
    void equalsAndHashCode_workCorrectly() {
        HttpClientConfig c1 = HttpClientConfig.defaults();
        HttpClientConfig c2 = HttpClientConfig.defaults();

        assertEquals(c1, c2);
        assertEquals(c1.hashCode(), c2.hashCode());
    }

    @Test
    void toString_returnsNonEmptyString() {
        HttpClientConfig config = HttpClientConfig.defaults();
        String str = config.toString();

        assertNotNull(str);
        assertFalse(str.isEmpty());
    }
}
