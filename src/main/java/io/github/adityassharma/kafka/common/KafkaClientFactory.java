package io.github.adityassharma.kafka.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Factory that constructs {@link KafkaConsumer} and {@link KafkaProducer}
 * instances from an {@link AppProperties} configuration.
 *
 * <p>All standard Kafka client properties present in the properties file are
 * forwarded to the client verbatim (prefixed keys are NOT used — the property
 * file uses the exact Kafka property names).  Application-specific properties
 * (num.consumer.threads, topic.name, etc.) are ignored by the Kafka client
 * because it only reads keys it knows about.
 *
 * <p>Thread-safety: each call returns a <em>new</em> client instance.
 * KafkaConsumer is NOT thread-safe and must not be shared across threads.
 * KafkaProducer IS thread-safe and can be shared, but this factory creates
 * one per thread for isolation and clean shutdown semantics.
 */
public final class KafkaClientFactory {

    private KafkaClientFactory() {}

    // -----------------------------------------------------------------------
    // Consumer
    // -----------------------------------------------------------------------

    /**
     * Build a {@link KafkaConsumer<String, String>} from the supplied properties.
     *
     * <p>The following keys are read from the properties file:
     * <ul>
     *   <li>bootstrap.servers (required)</li>
     *   <li>group.id (required)</li>
     *   <li>key.deserializer / value.deserializer</li>
     *   <li>auto.offset.reset, enable.auto.commit, auto.commit.interval.ms</li>
     *   <li>session.timeout.ms, heartbeat.interval.ms</li>
     *   <li>max.poll.records, max.poll.interval.ms</li>
     *   <li>fetch.min.bytes, fetch.max.wait.ms, max.partition.fetch.bytes</li>
     * </ul>
     */
    public static KafkaConsumer<String, String> createConsumer(AppProperties appProps) {
        Properties kafkaProps = new Properties();

        // Copy all standard Kafka consumer properties by passing them directly.
        // The consumer client ignores unknown keys, so it is safe to pass the
        // entire properties file — no filtering needed.
        copyKafkaConsumerProps(kafkaProps, appProps);

        return new KafkaConsumer<>(kafkaProps);
    }

    // -----------------------------------------------------------------------
    // Producer
    // -----------------------------------------------------------------------

    /**
     * Build a {@link KafkaProducer<String, String>} from the supplied properties.
     *
     * <p>The following keys are read from the properties file:
     * <ul>
     *   <li>bootstrap.servers (required)</li>
     *   <li>key.serializer / value.serializer</li>
     *   <li>acks, retries, retry.backoff.ms</li>
     *   <li>batch.size, linger.ms, buffer.memory</li>
     *   <li>compression.type</li>
     *   <li>request.timeout.ms, delivery.timeout.ms, max.block.ms</li>
     *   <li>enable.idempotence</li>
     * </ul>
     */
    public static KafkaProducer<String, String> createProducer(AppProperties appProps) {
        Properties kafkaProps = new Properties();
        copyKafkaProducerProps(kafkaProps, appProps);
        return new KafkaProducer<>(kafkaProps);
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    private static void copyKafkaConsumerProps(Properties target, AppProperties src) {
        // Required
        copyRequired(target, src, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
        copyRequired(target, src, ConsumerConfig.GROUP_ID_CONFIG);

        // Serialization
        copyWithDefault(target, src,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");
        copyWithDefault(target, src,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");

        // Offset
        copyWithDefault(target, src, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        copyWithDefault(target, src, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        copyOptional(target, src, ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);

        // Session / heartbeat
        copyOptional(target, src, ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
        copyOptional(target, src, ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);

        // Polling
        copyOptional(target, src, ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
        copyOptional(target, src, ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);

        // Fetch
        copyOptional(target, src, ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
        copyOptional(target, src, ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
        copyOptional(target, src, ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG);
    }

    private static void copyKafkaProducerProps(Properties target, AppProperties src) {
        // Required
        copyRequired(target, src, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);

        // Serialization
        copyWithDefault(target, src,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer");
        copyWithDefault(target, src,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer");

        // Durability
        copyWithDefault(target, src, ProducerConfig.ACKS_CONFIG, "1");
        copyOptional(target, src, ProducerConfig.RETRIES_CONFIG);
        copyOptional(target, src, ProducerConfig.RETRY_BACKOFF_MS_CONFIG);

        // Batching
        copyOptional(target, src, ProducerConfig.BATCH_SIZE_CONFIG);
        copyOptional(target, src, ProducerConfig.LINGER_MS_CONFIG);
        copyOptional(target, src, ProducerConfig.BUFFER_MEMORY_CONFIG);

        // Compression
        copyWithDefault(target, src, ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");

        // Timeouts
        copyOptional(target, src, ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        copyOptional(target, src, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        copyOptional(target, src, ProducerConfig.MAX_BLOCK_MS_CONFIG);

        // Idempotence
        copyOptional(target, src, ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
    }

    private static void copyRequired(Properties target, AppProperties src, String key) {
        target.setProperty(key, src.getRequired(key));
    }

    private static void copyWithDefault(Properties target, AppProperties src,
                                        String key, String defaultVal) {
        target.setProperty(key, src.get(key, defaultVal));
    }

    private static void copyOptional(Properties target, AppProperties src, String key) {
        Properties raw = src.getRawProperties();
        String val = raw.getProperty(key);
        if (val != null && !val.isBlank()) {
            target.setProperty(key, val.trim());
        }
    }
}
