package io.github.adityassharma.kafka.common;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Stateless utility for converting between JSON strings and Avro {@link GenericRecord}.
 *
 * <h2>JSON → GenericRecord ({@link #fromJson})</h2>
 * <p>Used on the producer side: the raw JSON fetched from the HTTP source is decoded
 * against the supplied Avro schema.  Field names in the JSON must match Avro field
 * names exactly.
 *
 * <h2>GenericRecord → JSON ({@link #toJson})</h2>
 * <p>Used on the consumer side before indexing to Elasticsearch: the deserialized
 * GenericRecord is re-encoded to JSON using Avro's JsonEncoder, producing standard
 * JSON that Elasticsearch can parse.
 */
public final class AvroConverter {

    private AvroConverter() {}

    /**
     * Decode a JSON string into an Avro {@link GenericRecord} using the supplied schema.
     *
     * @param json   raw JSON string (e.g. from HTTP feed)
     * @param schema Avro schema the JSON must conform to
     * @return decoded GenericRecord
     * @throws RuntimeException on parse failure
     */
    public static GenericRecord fromJson(String json, Schema schema) {
        try {
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            return reader.read(null, DecoderFactory.get().jsonDecoder(schema, json));
        } catch (IOException e) {
            throw new RuntimeException(
                "Failed to convert JSON to Avro GenericRecord: " + e.getMessage(), e);
        }
    }

    /**
     * Encode an Avro {@link GenericRecord} to a JSON string using Avro's JsonEncoder.
     * Output is valid JSON compatible with Elasticsearch.
     *
     * @param record the GenericRecord to encode
     * @return JSON string representation
     * @throws RuntimeException on encoding failure
     */
    public static String toJson(GenericRecord record) {
        try {
            Schema schema = record.getSchema();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, baos);
            new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
            encoder.flush();
            return baos.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(
                "Failed to convert Avro GenericRecord to JSON: " + e.getMessage(), e);
        }
    }
}
