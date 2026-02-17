package io.flink.connector.http.model;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HttpSinkRecordTest {

    @Test
    void builder_createsRecordWithAllFields() {
        Map<String, String> headers = Map.of("Content-Type", "application/json");
        Map<String, Object> body = Map.of("payload", "data");

        HttpSinkRecord record = HttpSinkRecord.builder()
                .method("POST")
                .url("https://example.com/ingest")
                .headers(headers)
                .body(body)
                .build();

        assertEquals("POST", record.getMethod());
        assertEquals("https://example.com/ingest", record.getUrl());
        assertEquals(headers, record.getHeaders());
        assertEquals(body, record.getBody());
    }

    @Test
    void getHeaders_returnsEmptyMap_whenHeadersIsNull() {
        HttpSinkRecord record = HttpSinkRecord.builder()
                .method("GET")
                .url("https://example.com")
                .headers(null)
                .body(null)
                .build();

        assertEquals(Collections.emptyMap(), record.getHeaders());
    }

    @Test
    void getBody_returnsEmptyMap_whenBodyIsNull() {
        HttpSinkRecord record = HttpSinkRecord.builder()
                .method("GET")
                .url("https://example.com")
                .headers(Collections.emptyMap())
                .body(null)
                .build();

        assertEquals(Collections.emptyMap(), record.getBody());
    }

    @Test
    void getSizeInBytes_returnsZero_whenBodyIsEmpty() {
        HttpSinkRecord record = HttpSinkRecord.builder()
                .method("POST")
                .url("https://example.com")
                .headers(Collections.emptyMap())
                .body(Collections.emptyMap())
                .build();

        assertEquals(0, record.getSizeInBytes());
    }

    @Test
    void getSizeInBytes_returnsCorrectSize_whenBodyHasContent() {
        Map<String, Object> body = Map.of("key", "value");
        HttpSinkRecord record = HttpSinkRecord.builder()
                .method("POST")
                .url("https://example.com")
                .headers(Collections.emptyMap())
                .body(body)
                .build();

        long size = record.getSizeInBytes();
        assertEquals("{\"key\":\"value\"}".getBytes().length, size);
    }

    @Test
    void equalsAndHashCode_workCorrectly() {
        HttpSinkRecord record1 = HttpSinkRecord.builder()
                .method("GET")
                .url("https://example.com")
                .headers(Map.of("A", "1"))
                .body(Map.of("x", 1))
                .build();

        HttpSinkRecord record2 = HttpSinkRecord.builder()
                .method("GET")
                .url("https://example.com")
                .headers(Map.of("A", "1"))
                .body(Map.of("x", 1))
                .build();

        assertEquals(record1, record2);
        assertEquals(record1.hashCode(), record2.hashCode());
    }
}
