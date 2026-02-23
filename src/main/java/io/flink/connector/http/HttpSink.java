package io.flink.connector.http;


import io.flink.connector.http.config.HttpSinkConfig;
import io.flink.connector.http.model.HttpSinkRecord;
import io.flink.connector.http.sink.HttpSinkBase;
import io.flink.connector.http.sink.writer.HttpSinkWriter;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

/**
 * Flink sink that sends upstream elements as HTTP requests using async buffering.
 *
 * <p>Extends {@link HttpSinkBase} and uses the buffering protocol from
 * {@link org.apache.flink.connector.base.sink.AsyncSinkBase}. Each element is converted to an
 * {@link HttpSinkRecord} via an {@link ElementConverter}; records are batched and sent asynchronously
 * by {@link HttpSinkWriter}.
 *
 * <p>Example usage with an element converter:
 *
 * <pre>{@code
 * HttpSink<String> sink = new HttpSink<>(
 *     (element, context) -> HttpSinkRecord.builder()
 *         .method("POST")
 *         .url("https://example.com/ingest")
 *         .headers(Map.of("Content-Type", "application/json"))
 *         .body(Map.of("payload", element))
 *         .build(),
 *     HttpSinkConfig.defaults());
 * }</pre>
 *
 * @param <InputT> type of the elements to convert and send via HTTP
 */
@PublicEvolving
public class HttpSink<InputT> extends HttpSinkBase<InputT> {

    /**
     * Creates an HTTP sink with the given element converter and configuration.
     *
     * @param elementConverter converts upstream elements to {@link HttpSinkRecord}
     * @param httpSinkConfig sink configuration
     */
    public HttpSink(
            ElementConverter<InputT, HttpSinkRecord> elementConverter,
            HttpSinkConfig httpSinkConfig) {
        super(elementConverter, httpSinkConfig);
    }
}
