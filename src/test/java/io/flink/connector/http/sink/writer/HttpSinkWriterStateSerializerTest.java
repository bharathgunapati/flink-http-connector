package io.flink.connector.http.sink.writer;

import io.flink.connector.http.model.HttpSinkRecord;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HttpSinkWriterStateSerializerTest {

    private final HttpSinkWriterStateSerializer serializer = new HttpSinkWriterStateSerializer();

    @Test
    void getVersion_returnsCurrentVersion() {
        assertEquals(3, serializer.getVersion());
    }

    @Test
    void serializeAndDeserialize_emptyState_roundtrips() throws Exception {
        BufferedRequestState<HttpSinkRecord> emptyState = BufferedRequestState.emptyState();

        byte[] serialized = serializer.serialize(emptyState);
        assertNotNull(serialized);
        assertTrue(serialized.length > 0);

        BufferedRequestState<HttpSinkRecord> deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);
        assertNotNull(deserialized);
        assertTrue(deserialized.getBufferedRequestEntries().isEmpty());
    }

    @Test
    void serializeAndDeserialize_stateWithRecord_roundtrips() throws Exception {
        HttpSinkRecord record =
                HttpSinkRecord.builder()
                        .method("POST")
                        .url("https://example.com/ingest")
                        .headers(Map.of("Content-Type", "application/json"))
                        .body(Map.of("key", "value"))
                        .build();

        RequestEntryWrapper<HttpSinkRecord> wrapper = new RequestEntryWrapper<>(record, 0);
        BufferedRequestState<HttpSinkRecord> state =
                new BufferedRequestState<>(List.of(wrapper));

        byte[] serialized = serializer.serialize(state);
        assertNotNull(serialized);
        assertTrue(serialized.length > 0);

        BufferedRequestState<HttpSinkRecord> deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);
        assertNotNull(deserialized);
        assertEquals(1, deserialized.getBufferedRequestEntries().size());
        assertEquals(record, deserialized.getBufferedRequestEntries().get(0).getRequestEntry());
    }

    @Test
    void serializeAndDeserialize_stateWithEmptyBody_roundtrips() throws Exception {
        HttpSinkRecord record =
                HttpSinkRecord.builder()
                        .method("GET")
                        .url("https://example.com/data")
                        .headers(Collections.emptyMap())
                        .body(Collections.emptyMap())
                        .build();

        RequestEntryWrapper<HttpSinkRecord> wrapper = new RequestEntryWrapper<>(record, 0);
        BufferedRequestState<HttpSinkRecord> state =
                new BufferedRequestState<>(List.of(wrapper));

        byte[] serialized = serializer.serialize(state);
        BufferedRequestState<HttpSinkRecord> deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertEquals(1, deserialized.getBufferedRequestEntries().size());
        HttpSinkRecord deserializedRecord =
                deserialized.getBufferedRequestEntries().get(0).getRequestEntry();
        assertEquals(record.getMethod(), deserializedRecord.getMethod());
        assertEquals(record.getUrl(), deserializedRecord.getUrl());
    }
}
