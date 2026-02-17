package io.flink.connector.http.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        assertNull(config.getProxyHost());
        assertNull(config.getProxyPort());
        assertEquals("http", config.getProxyScheme());
        assertNull(config.getTrustStorePath());
        assertNull(config.getKeyStorePath());
        assertFalse(config.isProxyConfigured());
        assertFalse(config.isTrustStoreConfigured());
        assertFalse(config.isKeyStoreConfigured());
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

    @Test
    void builder_withProxyConfig_setsProxyFields() {
        HttpClientConfig config =
                HttpClientConfig.builder()
                        .proxyHost("proxy.example.com")
                        .proxyPort(8080)
                        .proxyScheme("https")
                        .build();

        assertTrue(config.isProxyConfigured());
        assertEquals("proxy.example.com", config.getProxyHost());
        assertEquals(8080, config.getProxyPort());
        assertEquals("https", config.getProxyScheme());
    }

    @Test
    void builder_withTrustAndKeyStore_setsSslFields() {
        HttpClientConfig config =
                HttpClientConfig.builder()
                        .trustStorePath("/path/to/truststore.jks")
                        .trustStorePassword("trust-secret")
                        .keyStorePath("/path/to/keystore.p12")
                        .keyStorePassword("key-secret")
                        .build();

        assertTrue(config.isTrustStoreConfigured());
        assertTrue(config.isKeyStoreConfigured());
        assertEquals("/path/to/truststore.jks", config.getTrustStorePath());
        assertEquals("trust-secret", config.getTrustStorePassword());
        assertEquals("/path/to/keystore.p12", config.getKeyStorePath());
        assertEquals("key-secret", config.getKeyStorePassword());
    }

    @Test
    void isProxyConfigured_returnsFalse_whenOnlyProxyHostSet() {
        HttpClientConfig config =
                HttpClientConfig.builder().proxyHost("proxy.example.com").build();
        assertFalse(config.isProxyConfigured());
    }

    @Test
    void isProxyConfigured_returnsFalse_whenOnlyProxyPortSet() {
        HttpClientConfig config = HttpClientConfig.builder().proxyPort(8080).build();
        assertFalse(config.isProxyConfigured());
    }

    @Test
    void isProxyConfigured_returnsFalse_whenProxyHostEmpty() {
        HttpClientConfig config =
                HttpClientConfig.builder()
                        .proxyHost("")
                        .proxyPort(8080)
                        .build();
        assertFalse(config.isProxyConfigured());
    }

    @Test
    void isTrustStoreConfigured_returnsFalse_whenPathEmpty() {
        HttpClientConfig config =
                HttpClientConfig.builder().trustStorePath("").build();
        assertFalse(config.isTrustStoreConfigured());
    }

    @Test
    void isKeyStoreConfigured_returnsFalse_whenPathEmpty() {
        HttpClientConfig config =
                HttpClientConfig.builder().keyStorePath("").build();
        assertFalse(config.isKeyStoreConfigured());
    }
}
