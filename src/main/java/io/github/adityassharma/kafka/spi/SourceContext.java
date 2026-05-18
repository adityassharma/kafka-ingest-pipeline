package io.github.adityassharma.kafka.spi;

import java.util.Properties;

/**
 * Provided by the pipeline framework to a {@link Source} on startup.
 * Sources use this to emit records into the pipeline without directly
 * coupling to a KafkaProducer or any specific serialisation format.
 */
public interface SourceContext {

    /**
     * Publish a JSON record to the pipeline on the given topic.
     * The framework handles serialisation (JSON, Avro, etc.) transparently.
     *
     * @param topic      destination Kafka topic
     * @param jsonRecord the record payload as a UTF-8 JSON string
     */
    void emit(String topic, String jsonRecord);

    /**
     * Returns the configuration properties scoped to this source instance
     * (prefix stripped — keys are plain names like {@code url}, {@code topic}).
     */
    Properties config();
}
