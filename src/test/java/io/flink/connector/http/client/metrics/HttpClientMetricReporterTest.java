package io.flink.connector.http.client.metrics;

import org.apache.flink.metrics.MetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class HttpClientMetricReporterTest {

    @Mock
    private MetricGroup metricGroup;

    @Test
    void constructor_withMetricGroupAndName_setsFields() {
        HttpClientMetricReporter reporter =
                new HttpClientMetricReporter(metricGroup, "http-client-metrics");

        assertNotNull(reporter);
        assertEquals(metricGroup, reporter.getMetricGroup());
        assertEquals("http-client-metrics", reporter.getMetricGroupName());
    }

    @Test
    void constructor_withLabelMap_delegatesToPrimaryConstructor() {
        HttpClientMetricReporter reporter =
                new HttpClientMetricReporter(
                        metricGroup, "http-client-metrics", Map.of("label", "value"));

        assertNotNull(reporter);
        assertEquals(metricGroup, reporter.getMetricGroup());
        assertEquals("http-client-metrics", reporter.getMetricGroupName());
    }

    @Test
    void constructor_withEmptyLabelMap_createsReporter() {
        HttpClientMetricReporter reporter =
                new HttpClientMetricReporter(metricGroup, "metrics", Map.of());

        assertNotNull(reporter);
        assertEquals("metrics", reporter.getMetricGroupName());
    }
}
