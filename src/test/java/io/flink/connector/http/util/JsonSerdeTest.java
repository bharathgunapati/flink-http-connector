package io.flink.connector.http.util;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonSerdeTest {

    @Test
    void toJson_returnsEmptyObject_whenBodyIsNull() {
        assertEquals("{}", JsonSerde.toJson(null));
    }

    @Test
    void toJson_returnsEmptyObject_whenBodyIsEmpty() {
        assertEquals("{}", JsonSerde.toJson(Collections.emptyMap()));
    }

    @Test
    void toJson_serializesSimpleMap() {
        Map<String, Object> body = Map.of("key", "value");
        assertEquals("{\"key\":\"value\"}", JsonSerde.toJson(body));
    }

    @Test
    void toJson_serializesNestedMap() {
        Map<String, Object> body = Map.of(
                "outer", Map.of("inner", "value"));
        String json = JsonSerde.toJson(body);
        assertTrue(json.contains("\"outer\""));
        assertTrue(json.contains("\"inner\""));
        assertTrue(json.contains("\"value\""));
    }

    @Test
    void toJson_serializesNumbersAndBooleans() {
        Map<String, Object> body = Map.of(
                "count", 42,
                "active", true);
        String json = JsonSerde.toJson(body);
        assertTrue(json.contains("42"));
        assertTrue(json.contains("true"));
    }

    @Test
    void sizeInBytes_returnsZero_whenBodyIsNull() {
        assertEquals(0, JsonSerde.sizeInBytes(null));
    }

    @Test
    void sizeInBytes_returnsZero_whenBodyIsEmpty() {
        assertEquals(0, JsonSerde.sizeInBytes(Collections.emptyMap()));
    }

    @Test
    void sizeInBytes_returnsCorrectLength() {
        Map<String, Object> body = Map.of("key", "value");
        long size = JsonSerde.sizeInBytes(body);
        assertEquals("{\"key\":\"value\"}".getBytes().length, size);
    }

    @Test
    void sizeInBytes_matchesToJsonLength() {
        Map<String, Object> body = Map.of("a", 1, "b", "test");
        long size = JsonSerde.sizeInBytes(body);
        assertEquals(JsonSerde.toJson(body).getBytes().length, size);
    }

    @Test
    void toJson_nonSerializableValue_throwsIllegalArgumentException() {
        // Object with getter that throws - Jackson wraps in JsonMappingException (extends JsonProcessingException)
        Map<String, Object> body = Map.of("key", new Object() {
            @SuppressWarnings("unused")
            public String getValue() {
                throw new RuntimeException("cannot serialize");
            }
        });
        assertThrows(IllegalArgumentException.class, () -> JsonSerde.toJson(body));
    }

    @Test
    void sizeInBytes_nonSerializableValue_throwsIllegalArgumentException() {
        Map<String, Object> body = Map.of("key", new Object() {
            @SuppressWarnings("unused")
            public String getValue() {
                throw new RuntimeException("cannot serialize");
            }
        });
        assertThrows(IllegalArgumentException.class, () -> JsonSerde.sizeInBytes(body));
    }
}
