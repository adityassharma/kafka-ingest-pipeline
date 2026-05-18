package io.github.adityassharma.kafka.pipeline;

import io.github.adityassharma.kafka.common.AppProperties;
import io.github.adityassharma.kafka.common.AvroConverter;
import io.github.adityassharma.kafka.common.KafkaClientFactory;
import io.github.adityassharma.kafka.common.MessageFormat;
import io.github.adityassharma.kafka.common.SchemaLoader;
import io.github.adityassharma.kafka.spi.Source;
import io.github.adityassharma.kafka.spi.SourceContext;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Manages the lifecycle of a single {@link Source} instance:
 * creates the Kafka producer, wires the {@link SourceContext}, and
 * runs the source in a dedicated executor thread.
 *
 * <p>A single KafkaProducer (thread-safe) is shared for all emit calls.
 * The producer handles JSON, Avro file-based, and Schema Registry formats
 * transparently — the Source only ever calls {@code context.emit(topic, json)}.
 */
public class SourceRunner {

    private static final Logger LOG = LogManager.getLogger(SourceRunner.class);

    private final String        name;
    private final Source        source;
    private final AppProperties appProps;
    private final Properties    sourceConfig;

    private ExecutorService executor;
    private final List<Closeable> producersToClose = new ArrayList<>();
    private BiConsumer<String, String> publishFn;

    public SourceRunner(String name, Source source, AppProperties appProps, Properties sourceConfig) {
        this.name         = name;
        this.source       = source;
        this.appProps     = appProps;
        this.sourceConfig = sourceConfig;
    }

    /** Build the Kafka producer and start the source in a background thread. */
    public void start() {
        publishFn = buildPublishFn();
        executor  = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "source-" + name);
            t.setDaemon(false);
            return t;
        });

        SourceContext context = new SourceContext() {
            @Override
            public void emit(String topic, String jsonRecord) {
                publishFn.accept(topic, jsonRecord);
            }
            @Override
            public Properties config() {
                return sourceConfig;
            }
        };

        executor.submit(() -> {
            try {
                source.start(context);
            } catch (Exception e) {
                LOG.error("Source '{}' terminated with error: {}", name, e.getMessage(), e);
            }
        });

        LOG.info("SourceRunner '{}' started (type={})", name, source.type());
    }

    /** Signal graceful shutdown: stop the source, flush/close the producer. */
    public void shutdown() {
        LOG.info("Shutting down SourceRunner '{}'", name);
        source.stop();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(15, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        for (Closeable c : producersToClose) {
            try { c.close(); } catch (Exception e) {
                LOG.warn("Error closing producer for source '{}': {}", name, e.getMessage());
            }
        }
        try { source.close(); } catch (Exception e) {
            LOG.warn("Error closing source '{}': {}", name, e.getMessage());
        }
        LOG.info("SourceRunner '{}' shut down", name);
    }

    @SuppressWarnings("resource")
    private BiConsumer<String, String> buildPublishFn() {
        MessageFormat format = MessageFormat.from(appProps.get("message.format", "json"));

        switch (format) {
            case JSON -> {
                KafkaProducer<String, String> producer = KafkaClientFactory.createProducer(appProps);
                producersToClose.add(producer);
                return (topic, json) -> producer.send(
                    new ProducerRecord<>(topic, json),
                    (meta, ex) -> {
                        if (ex != null) LOG.error("Source '{}' produce failed: {}", name, ex.getMessage());
                    }
                );
            }
            case AVRO -> {
                Schema schema = SchemaLoader.fromFile(appProps);
                if (appProps.get("message.schema.registry.url", null) != null) {
                    KafkaProducer<String, GenericRecord> producer =
                        KafkaClientFactory.createSchemaRegistryProducer(appProps);
                    producersToClose.add(producer);
                    return (topic, json) -> {
                        GenericRecord record = AvroConverter.fromJson(json, schema);
                        producer.send(new ProducerRecord<>(topic, record),
                            (meta, ex) -> {
                                if (ex != null) LOG.error("Source '{}' produce failed: {}", name, ex.getMessage());
                            });
                    };
                } else {
                    KafkaProducer<String, GenericRecord> producer =
                        KafkaClientFactory.createAvroProducer(appProps, schema);
                    producersToClose.add(producer);
                    return (topic, json) -> {
                        GenericRecord record = AvroConverter.fromJson(json, schema);
                        producer.send(new ProducerRecord<>(topic, record),
                            (meta, ex) -> {
                                if (ex != null) LOG.error("Source '{}' produce failed: {}", name, ex.getMessage());
                            });
                    };
                }
            }
            default -> throw new IllegalArgumentException("Unsupported message.format: " + format);
        }
    }
}
