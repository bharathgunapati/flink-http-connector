package io.flink.connector.http.client.apache;

import io.flink.connector.http.config.HttpClientConfig;
import io.flink.connector.http.config.RetryConfig;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.hc.client5.http.HttpRoute;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestProducer;
import org.apache.hc.client5.http.async.methods.SimpleResponseConsumer;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.impl.routing.DefaultProxyRoutePlanner;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.pool.ConnPoolControl;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

import javax.net.ssl.SSLContext;
import java.io.File;

/**
 * Wrapper around Apache's CloseableHttpAsyncClient for asynchronous HTTP requests. Manages connection
 * pooling, timeouts, and metrics.
 *
 * <p>Used internally by {@link ApacheHttpRequestExecutor}. Not part of the public client API.
 *
 * @see ApacheHttpRequestExecutor
 * @see CloseableHttpAsyncClient
 */
public class HttpClient {

  private CloseableHttpAsyncClient client;
  private PoolingAsyncClientConnectionManager cm;
  private final int maxConnectionsTotal;
  private final int maxConnectionsPerRoute;
  private final int responseTimeoutInMillis;
  private final int socketTimeoutInMillis;
  private final int connectTimeoutInMillis;
  private final int evictIdleConnectionTimeoutInMillis;
  private final HttpClientConfig httpClientConfig;

  public HttpClient(MetricGroup metricGroup, HttpClientConfig httpClientConfig, RetryConfig retryConfig) {
    this.httpClientConfig = httpClientConfig;
    this.connectTimeoutInMillis = httpClientConfig.getConnectTimeoutMs();
    this.responseTimeoutInMillis = httpClientConfig.getResponseTimeoutMs();
    this.socketTimeoutInMillis = httpClientConfig.getSocketTimeoutMs();
    this.maxConnectionsTotal = httpClientConfig.getMaxConnections();
    this.maxConnectionsPerRoute = httpClientConfig.getMaxConnectionsPerHost();
    this.evictIdleConnectionTimeoutInMillis = httpClientConfig.getEvictIdleConnectionTimeoutMs();
    initHttpClient(metricGroup, retryConfig);
  }

  private void initHttpClient(MetricGroup metricGroup, RetryConfig retryConfig) {
    cm = getPoolingAsyncClientConnectionManager();
    IOReactorConfig ioReactorConfig =
        IOReactorConfig.custom().setSoTimeout(Timeout.ofMilliseconds(socketTimeoutInMillis)).build();
    registerConnectionPoolMetrics(metricGroup, cm);

    var clientBuilder =
        HttpAsyncClients.custom()
            .setDefaultRequestConfig(
                RequestConfig.custom()
                    .setResponseTimeout(Timeout.ofMilliseconds(responseTimeoutInMillis))
                    .build())
            .setConnectionManager(cm)
            .setIOReactorConfig(ioReactorConfig)
            .evictExpiredConnections()
            .setUserAgent("flink-http-connector")
            .evictIdleConnections(TimeValue.ofMilliseconds(evictIdleConnectionTimeoutInMillis))
            .setRetryStrategy(new HttpRequestRetryStrategy(retryConfig));

    if (httpClientConfig.isProxyConfigured()) {
      HttpHost proxy =
          new HttpHost(
              httpClientConfig.getProxyScheme(),
              httpClientConfig.getProxyHost(),
              httpClientConfig.getProxyPort());
      clientBuilder.setRoutePlanner(new DefaultProxyRoutePlanner(proxy));
    }

    this.client = clientBuilder.build();
    this.client.start();
  }

  private PoolingAsyncClientConnectionManager getPoolingAsyncClientConnectionManager() {
    var builder =
        PoolingAsyncClientConnectionManagerBuilder.create()
            .setConnectionConfigResolver(
                route ->
                    ConnectionConfig.custom()
                        .setConnectTimeout(Timeout.ofMilliseconds(connectTimeoutInMillis))
                        .setSocketTimeout(Timeout.ofMilliseconds(socketTimeoutInMillis))
                        .setValidateAfterInactivity(TimeValue.ofMinutes(1))
                        .build())
            .setMaxConnPerRoute(maxConnectionsPerRoute)
            .setMaxConnTotal(maxConnectionsTotal);

    if (httpClientConfig.isTrustStoreConfigured() || httpClientConfig.isKeyStoreConfigured()) {
      SSLContext sslContext = buildSSLContext();
      builder.setTlsStrategy(
          ClientTlsStrategyBuilder.create().setSslContext(sslContext).build());
    }

    return builder.build();
  }

  private SSLContext buildSSLContext() {
    try {
      var sslBuilder = SSLContexts.custom();

      if (httpClientConfig.isTrustStoreConfigured()) {
        File trustStore = new File(httpClientConfig.getTrustStorePath());
        char[] trustPassword =
            httpClientConfig.getTrustStorePassword() != null
                ? httpClientConfig.getTrustStorePassword().toCharArray()
                : null;
        sslBuilder.loadTrustMaterial(trustStore, trustPassword);
      } else if (httpClientConfig.isKeyStoreConfigured()) {
        // Client cert only: use default trust store (JVM cacerts)
        sslBuilder.loadTrustMaterial(
            (java.security.KeyStore) null, (org.apache.hc.core5.ssl.TrustStrategy) null);
      }

      if (httpClientConfig.isKeyStoreConfigured()) {
        File keyStore = new File(httpClientConfig.getKeyStorePath());
        char[] keyPassword =
            httpClientConfig.getKeyStorePassword() != null
                ? httpClientConfig.getKeyStorePassword().toCharArray()
                : null;
        sslBuilder.loadKeyMaterial(keyStore, keyPassword, keyPassword);
      }

      return sslBuilder.build();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to build SSL context from configured trust/key store", e);
    }
  }

  private void registerConnectionPoolMetrics(
      MetricGroup metricGroup, ConnPoolControl<HttpRoute> connPoolControl) {
    metricGroup.gauge(
        "httpclient.pool.total.max",
        (Gauge<Long>) () -> (long) connPoolControl.getTotalStats().getMax());

    metricGroup.gauge(
        "httpclient.pool.total.connections.available",
        (Gauge<Long>) () -> (long) connPoolControl.getTotalStats().getAvailable());

    metricGroup.gauge(
        "httpclient.pool.total.connections.leased",
        (Gauge<Long>) () -> (long) connPoolControl.getTotalStats().getLeased());

    metricGroup.gauge(
        "httpclient.pool.total.pending",
        (Gauge<Long>) () -> (long) connPoolControl.getTotalStats().getPending());

    metricGroup.gauge(
        "httpclient.pool.route.max.default",
        (Gauge<Long>) () -> (long) connPoolControl.getDefaultMaxPerRoute());
  }

  public void executeRequest(
      SimpleRequestProducer simpleRequestProducer,
      SimpleResponseConsumer simpleResponseConsumer,
      FutureCallback<SimpleHttpResponse> futureCallback) {
    client.execute(simpleRequestProducer, simpleResponseConsumer, futureCallback);
  }

  public void executeRequest(
      SimpleHttpRequest simpleHttpRequest, FutureCallback<SimpleHttpResponse> futureCallback) {
    client.execute(simpleHttpRequest, futureCallback);
  }

  public void close() {
    cm.close(CloseMode.GRACEFUL);
    client.close(CloseMode.GRACEFUL);
  }
}
