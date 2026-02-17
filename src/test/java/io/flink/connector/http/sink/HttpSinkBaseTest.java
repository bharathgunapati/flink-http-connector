package io.flink.connector.http.sink;

import io.flink.connector.http.config.HttpSinkConfig;
import io.flink.connector.http.model.HttpSinkRecord;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HttpSinkBaseTest {

    @Test
    void getWriterStateSerializer_returnsSerializer() {
        ElementConverter<String, HttpSinkRecord> converter =
                (element, context) -> HttpSinkRecord.builder()
                        .method("POST")
                        .url("https://example.com")
                        .headers(java.util.Map.of())
                        .body(java.util.Map.of("payload", element))
                        .build();

        HttpSinkBase<String> sinkBase = new HttpSinkBase<>(converter, HttpSinkConfig.defaults());

        SimpleVersionedSerializer<BufferedRequestState<HttpSinkRecord>> serializer =
                sinkBase.getWriterStateSerializer();

        assertNotNull(serializer);
        assertTrue(serializer.getVersion() > 0);
    }

    @Test
    void getWriterStateSerializer_roundtripsEmptyState() throws Exception {
        ElementConverter<String, HttpSinkRecord> converter =
                (element, context) -> HttpSinkRecord.builder()
                        .method("POST")
                        .url("https://example.com")
                        .headers(java.util.Map.of())
                        .body(java.util.Map.of())
                        .build();

        HttpSinkBase<String> sinkBase = new HttpSinkBase<>(converter, HttpSinkConfig.defaults());
        SimpleVersionedSerializer<BufferedRequestState<HttpSinkRecord>> serializer =
                sinkBase.getWriterStateSerializer();

        BufferedRequestState<HttpSinkRecord> emptyState = BufferedRequestState.emptyState();
        byte[] serialized = serializer.serialize(emptyState);
        BufferedRequestState<HttpSinkRecord> deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertNotNull(deserialized);
        assertTrue(deserialized.getBufferedRequestEntries().isEmpty());
    }
}
