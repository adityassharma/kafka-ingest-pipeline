package io.github.adityassharma.kafka.common;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
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
 *
 * <h2>Format support</h2>
 * <ul>
 *   <li>{@link #createProducer} / {@link #createConsumer} — JSON (String value)</li>
 *   <li>{@link #createAvroProducer} / {@link #createAvroConsumer} — Avro binary,
 *       file-based schema, no Schema Registry required</li>
 *   <li>{@link #createSchemaRegistryProducer} / {@link #createSchemaRegistryConsumer}
 *       — Avro with Confluent Schema Registry</li>
 * </ul>
 */
public final class KafkaClientFactory {

    private KafkaClientFactory() {}

    // -----------------------------------------------------------------------
    // JSON (String) — existing behaviour unchanged
    // -----------------------------------------------------------------------

    /**
     * Build a {@link KafkaProducer} of type {@code KafkaProducer<String, String>}.
     */
    public static KafkaProducer<String, String> createProducer(AppProperties appProps) {
        Properties kafkaProps = new Properties();
        copyKafkaProducerProps(kafkaProps, appProps);
        return new KafkaProducer<>(kafkaProps);
    }

    /**
     * Build a {@link KafkaConsumer} of type {@code KafkaConsumer<String, String>}.
     */
    public static KafkaConsumer<String, String> createConsumer(AppProperties appProps) {
        Properties kafkaProps = new Properties();
        copyKafkaConsumerProps(kafkaProps, appProps);
        return new KafkaConsumer<>(kafkaProps);
    }

    // -----------------------------------------------------------------------
    // Avro — file-based schema, no Schema Registry
    // -----------------------------------------------------------------------

    /**
     * Build a {@link KafkaProducer} of type {@code KafkaProducer<String, GenericRecord>}
     * using Avro binary encoding with the supplied file-based schema.
     *
     * <p>Pair with {@link #createAvroConsumer} using the same schema file.
     */
    public static KafkaProducer<String, GenericRecord> createAvroProducer(
            AppProperties appProps, Schema schema) {
        Properties kafkaProps = new Properties();
        copyKafkaProducerProps(kafkaProps, appProps);
        // Remove class-name serializer entries — we pass instances directly below
        kafkaProps.remove(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        kafkaProps.remove(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        return new KafkaProducer<>(kafkaProps,
            new StringSerializer(),
            new AvroFileSerializer(schema));
    }

    /**
     * Build a {@link KafkaConsumer} of type {@code KafkaConsumer<String, GenericRecord>}
     * using Avro binary decoding with the supplied file-based schema.
     *
     * <p>Pair with {@link #createAvroProducer} using the same schema file.
     */
    public static KafkaConsumer<String, GenericRecord> createAvroConsumer(
            AppProperties appProps, Schema schema) {
        Properties kafkaProps = new Properties();
        copyKafkaConsumerProps(kafkaProps, appProps);
        kafkaProps.remove(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        kafkaProps.remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        return new KafkaConsumer<>(kafkaProps,
            new StringDeserializer(),
            new AvroFileDeserializer(schema));
    }

    // -----------------------------------------------------------------------
    // Avro — Confluent Schema Registry
    // -----------------------------------------------------------------------

    /**
     * Build a {@link KafkaProducer} of type {@code KafkaProducer<String, GenericRecord>}
     * using Confluent's {@code KafkaAvroSerializer}.  The schema is registered/fetched
     * from the registry automatically.
     *
     * <p>Requires {@code message.schema.registry.url} in the properties file.
     */
    @SuppressWarnings("resource") // avroSer lifetime is tied to the returned producer; closing it early would break serialization
    public static KafkaProducer<String, GenericRecord> createSchemaRegistryProducer(
            AppProperties appProps) {
        String registryUrl = appProps.getRequired("message.schema.registry.url");
        Properties kafkaProps = new Properties();
        copyKafkaProducerProps(kafkaProps, appProps);
        kafkaProps.remove(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        kafkaProps.remove(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);

        KafkaAvroSerializer avroSer = new KafkaAvroSerializer();
        avroSer.configure(Map.of("schema.registry.url", registryUrl), false /* isKey */);

        return new KafkaProducer<>(kafkaProps,
            new StringSerializer(),
            avroSer::serialize);
    }

    /**
     * Build a {@link KafkaConsumer} of type {@code KafkaConsumer<String, GenericRecord>}
     * using Confluent's {@code KafkaAvroDeserializer} in generic (non-specific) mode.
     *
     * <p>Requires {@code message.schema.registry.url} in the properties file.
     */
    @SuppressWarnings("resource") // avroDes lifetime is tied to the returned consumer; closing it early would break deserialization
    public static KafkaConsumer<String, GenericRecord> createSchemaRegistryConsumer(
            AppProperties appProps) {
        String registryUrl = appProps.getRequired("message.schema.registry.url");
        Properties kafkaProps = new Properties();
        copyKafkaConsumerProps(kafkaProps, appProps);
        kafkaProps.remove(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        kafkaProps.remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);

        KafkaAvroDeserializer avroDes = new KafkaAvroDeserializer();
        avroDes.configure(
            Map.of("schema.registry.url", registryUrl,
                   "specific.avro.reader", false),
            false /* isKey */);

        return new KafkaConsumer<>(kafkaProps,
            new StringDeserializer(),
            (topic, data) -> (GenericRecord) avroDes.deserialize(topic, data));
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

        // Security / TLS — forwarded only when present in the properties file.
        // Set security.protocol=SSL for one-way TLS (truststore only).
        // Add ssl.keystore.* properties to enable mTLS (mutual TLS).
        copySslProps(target, src);

        // SASL authentication — forwarded only when present in the properties file.
        // Set security.protocol=SASL_PLAINTEXT or SASL_SSL and provide
        // sasl.mechanism + sasl.jaas.config for username/password auth.
        copySaslProps(target, src);
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

        // Security / TLS — forwarded only when present in the properties file.
        // Set security.protocol=SSL for one-way TLS (truststore only).
        // Add ssl.keystore.* properties to enable mTLS (mutual TLS).
        copySslProps(target, src);

        // SASL authentication — forwarded only when present in the properties file.
        // Set security.protocol=SASL_PLAINTEXT or SASL_SSL and provide
        // sasl.mechanism + sasl.jaas.config for username/password auth.
        copySaslProps(target, src);
    }

    private static void copySslProps(Properties target, AppProperties src) {
        copyOptional(target, src, CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
        copyOptional(target, src, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        copyOptional(target, src, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        copyOptional(target, src, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        copyOptional(target, src, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        copyOptional(target, src, SslConfigs.SSL_KEY_PASSWORD_CONFIG);
        copyOptional(target, src, SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
    }

    private static void copySaslProps(Properties target, AppProperties src) {
        // Core SASL properties — required for any SASL mechanism
        copyOptional(target, src, SaslConfigs.SASL_MECHANISM);
        copyOptional(target, src, SaslConfigs.SASL_JAAS_CONFIG);

        // Kerberos (GSSAPI) — only needed when sasl.mechanism=GSSAPI
        copyOptional(target, src, SaslConfigs.SASL_KERBEROS_SERVICE_NAME);

        // Custom callback handler classes — advanced / optional
        copyOptional(target, src, SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS);
        copyOptional(target, src, SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS);
        copyOptional(target, src, SaslConfigs.SASL_LOGIN_CLASS);
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
