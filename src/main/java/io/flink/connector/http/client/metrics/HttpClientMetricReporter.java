package io.flink.connector.http.client.metrics;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.flink.metrics.MetricGroup;

import java.util.Map;

/**
 * Reports HTTP client metrics to a Flink {@link MetricGroup}.
 *
 * <p>Used to expose connection pool statistics and request/response metrics for monitoring.
 */
@Getter
@RequiredArgsConstructor
public class HttpClientMetricReporter {

    private final MetricGroup metricGroup;
    private final String metricGroupName;

    /**
     * Constructs a metric reporter with optional labels (for backward compatibility).
     *
     * @param metricGroup the Flink metric group
     * @param groupName the metric group name
     * @param labelMap optional labels (currently unused)
     */
    public HttpClientMetricReporter(MetricGroup metricGroup, String groupName, @SuppressWarnings("unused") Map<String, String> labelMap) {
        this(metricGroup, groupName);
    }
}
