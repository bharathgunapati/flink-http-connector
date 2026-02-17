package io.flink.connector.http.model;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HttpExchangeTest {

    @Test
    void builder_createsExchangeWithRecordAndResponse() {
        HttpSinkRecord record = HttpSinkRecord.builder()
                .method("POST")
                .url("https://example.com")
                .headers(Map.of())
                .body(Map.of("data", "value"))
                .build();

        HttpResponse response = HttpResponse.builder()
                .statusCode(200)
                .body("{\"id\":\"123\"}")
                .build();

        HttpExchange exchange = HttpExchange.builder()
                .httpSinkRecord(record)
                .response(response)
                .build();

        assertEquals(record, exchange.getHttpSinkRecord());
        assertEquals(response, exchange.getResponse());
    }

    @Test
    void equalsAndHashCode_workCorrectly() {
        HttpSinkRecord record = HttpSinkRecord.builder()
                .method("GET")
                .url("https://example.com")
                .headers(Map.of())
                .body(Map.of())
                .build();
        HttpResponse response = HttpResponse.builder().statusCode(200).body("ok").build();

        HttpExchange e1 = HttpExchange.builder().httpSinkRecord(record).response(response).build();
        HttpExchange e2 = HttpExchange.builder().httpSinkRecord(record).response(response).build();

        assertEquals(e1, e2);
        assertEquals(e1.hashCode(), e2.hashCode());
    }
}
