package io.flink.connector.http.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HttpResponseTest {

    @Test
    void builder_createsResponseWithStatusCodeAndBody() {
        HttpResponse response = HttpResponse.builder()
                .statusCode(200)
                .body("{\"result\":\"ok\"}")
                .build();

        assertEquals(200, response.getStatusCode());
        assertEquals("{\"result\":\"ok\"}", response.getBody());
    }

    @Test
    void builder_createsErrorResponseWithNullStatusCode() {
        HttpResponse response = HttpResponse.builder()
                .statusCode(null)
                .body("Connection failed")
                .build();

        assertNull(response.getStatusCode());
        assertEquals("Connection failed", response.getBody());
    }

    @Test
    void equalsAndHashCode_workCorrectly() {
        HttpResponse r1 = HttpResponse.builder().statusCode(200).body("ok").build();
        HttpResponse r2 = HttpResponse.builder().statusCode(200).body("ok").build();

        assertEquals(r1, r2);
        assertEquals(r1.hashCode(), r2.hashCode());
    }
}
