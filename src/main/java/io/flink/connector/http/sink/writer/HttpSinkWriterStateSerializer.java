package io.flink.connector.http.sink.writer;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.flink.connector.http.model.HttpSinkRecord;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Serializer for {@link HttpSinkRecord} used when checkpointing buffered requests in the HTTP sink
 * writer.
 *
 * <p>Uses Kryo for serialization. The version is bumped when the {@link HttpSinkRecord} contract
 * changes to ensure compatibility with existing checkpoints.
 *
 * @see org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer
 */
public class HttpSinkWriterStateSerializer extends AsyncSinkWriterStateSerializer<HttpSinkRecord> {

    /** {@inheritDoc} */
    @Override
    protected void serializeRequestToStream(HttpSinkRecord s, DataOutputStream out)
            throws IOException {
        Kryo kryo = new Kryo();
        kryo.register(HttpSinkRecord.class, new HttpSinkRecordKryoSerializer());
        Output output = new Output((OutputStream) out);
        kryo.writeObject(output, s);
        output.flush();
        output.close();
    }

    /** {@inheritDoc} */
    @Override
    protected HttpSinkRecord deserializeRequestFromStream(long requestSize, DataInputStream in)
            throws IOException {
        Kryo kryo = new Kryo();
        kryo.register(HttpSinkRecord.class, new HttpSinkRecordKryoSerializer());
        try (Input kryoInput = new Input(in)) {
            return kryo.readObject(kryoInput, HttpSinkRecord.class);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int getVersion() {
        return 3; // Bumped for body change: byte[] -> Map<String, Object> (Nashorn variable replacement)
    }
}
