package io.github.adityassharma.kafka.common;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

/**
 * Kafka {@link Deserializer} for Avro {@link GenericRecord} using file-based schema
 * (no Schema Registry).
 *
 * <p>Decodes Avro binary-encoded bytes using the schema loaded from a {@code .avsc} file.
 * Must be paired with {@link AvroFileSerializer} on the producer side using the same schema.
 */
public class AvroFileDeserializer implements Deserializer<GenericRecord> {

    private final Schema schema;

    public AvroFileDeserializer(Schema schema) {
        this.schema = schema;
    }

    @Override
    public GenericRecord deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            return new GenericDatumReader<GenericRecord>(schema).read(null, decoder);
        } catch (IOException e) {
            throw new RuntimeException("Avro binary deserialization failed for topic=" + topic, e);
        }
    }
}
