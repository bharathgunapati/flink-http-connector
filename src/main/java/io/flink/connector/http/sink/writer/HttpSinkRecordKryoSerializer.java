package io.flink.connector.http.sink.writer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.flink.connector.http.model.HttpSinkRecord;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Custom Kryo serializer for {@link HttpSinkRecord}.
 *
 * <p>Required because: (1) HttpSinkRecord has no no-arg constructor, and (2) Map.of() produces
 * immutable map types (e.g. ImmutableCollections$Map1) that Kryo cannot deserialize (no no-arg
 * constructor). This serializer writes maps as {@link HashMap} so they can be round-tripped.
 */
class HttpSinkRecordKryoSerializer extends Serializer<HttpSinkRecord> {

    @Override
    public void write(Kryo kryo, Output output, HttpSinkRecord record) {
        output.writeString(record.getMethod());
        output.writeString(record.getUrl());
        kryo.writeObject(output, toMutableHeadersMap(record.getHeaders()));
        kryo.writeObject(output, toMutableBodyMap(record.getBody()));
    }

    @Override
    @SuppressWarnings("unchecked")
    public HttpSinkRecord read(Kryo kryo, Input input, Class<HttpSinkRecord> type) {
        String method = input.readString();
        String url = input.readString();
        Map<String, String> headers = (Map<String, String>) kryo.readObject(input, HashMap.class);
        Map<String, Object> body = (Map<String, Object>) kryo.readObject(input, HashMap.class);
        return HttpSinkRecord.builder()
                .method(method)
                .url(url)
                .headers(headers != null ? headers : Collections.emptyMap())
                .body(body != null ? body : Collections.emptyMap())
                .build();
    }

    private static Map<String, String> toMutableHeadersMap(Map<String, String> map) {
        return map != null && !map.isEmpty() ? new HashMap<>(map) : new HashMap<>();
    }

    private static Map<String, Object> toMutableBodyMap(Map<String, Object> map) {
        return map != null && !map.isEmpty() ? new HashMap<>(map) : new HashMap<>();
    }
}
