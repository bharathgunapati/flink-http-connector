package io.flink.connector.http.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.UtilityClass;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Utility for JSON serialization of HTTP request bodies.
 *
 * <p>Serializes {@code Map<String, Object>} to JSON using Jackson. Used by the HTTP client layer
 * when building request payloads, keeping format concerns separate from the data model.
 */
@UtilityClass
public class JsonSerde {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Serializes the body map to a JSON string.
     *
     * @param body the body map (may be null or empty)
     * @return JSON string, or "{}" if body is null or empty
     * @throws IllegalArgumentException if serialization fails
     */
    public static String toJson(Map<String, Object> body) {
        if (body == null || body.isEmpty()) {
            return "{}";
        }
        try {
            return OBJECT_MAPPER.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize body to JSON", e);
        }
    }

    /**
     * Returns the size of the body in bytes when serialized to JSON.
     *
     * @param body the body map (may be null or empty)
     * @return size in bytes, or 0 if body is null or empty
     * @throws IllegalArgumentException if serialization fails
     */
    public static long sizeInBytes(Map<String, Object> body) {
        if (body == null || body.isEmpty()) {
            return 0;
        }
        try {
            return OBJECT_MAPPER.writeValueAsString(body).getBytes(StandardCharsets.UTF_8).length;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize body to JSON", e);
        }
    }
}
