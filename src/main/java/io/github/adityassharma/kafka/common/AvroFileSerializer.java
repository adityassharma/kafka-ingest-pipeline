package io.github.adityassharma.kafka.common;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Kafka {@link Serializer} for Avro {@link GenericRecord} using file-based schema
 * (no Schema Registry).
 *
 * <p>Encodes records using Avro binary encoding.  Both the producer and consumer
 * must use the same {@code .avsc} schema file for the encoding to be compatible.
 *
 * <p>Pair with {@link AvroFileDeserializer} on the consumer side.
 */
public class AvroFileSerializer implements Serializer<GenericRecord> {

    private final Schema schema;

    public AvroFileSerializer(Schema schema) {
        this.schema = schema;
    }

    @Override
    public byte[] serialize(String topic, GenericRecord data) {
        if (data == null) return null;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
            new GenericDatumWriter<GenericRecord>(schema).write(data, encoder);
            encoder.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Avro binary serialization failed for topic=" + topic, e);
        }
    }
}
