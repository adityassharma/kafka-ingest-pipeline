package io.github.adityassharma.kafka.pipeline;

import io.github.adityassharma.kafka.common.AppProperties;
import io.github.adityassharma.kafka.common.AvroConverter;
import io.github.adityassharma.kafka.common.KafkaClientFactory;
import io.github.adityassharma.kafka.common.MessageFormat;
import io.github.adityassharma.kafka.common.SchemaLoader;
import io.github.adityassharma.kafka.management.ComponentStatus;
import io.github.adityassharma.kafka.management.SinkStats;
import io.github.adityassharma.kafka.spi.Record;
import io.github.adityassharma.kafka.spi.Sink;
import io.github.adityassharma.kafka.spi.Transform;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Manages the lifecycle of a single {@link Sink} instance:
 * creates N KafkaConsumer worker threads, polls records, converts to
 * {@link Record}, calls {@link Sink#writeBatch}, routes failures to DLQ,
 * and commits offsets.
 *
 * <p>Each worker thread owns its own KafkaConsumer (not thread-safe).
 * The Sink instance is shared and must be thread-safe.
 *
 * <p>DLQ: if {@code dlq.topic} is present in sink config, a shared
 * KafkaProducer routes per-item failures there; otherwise failures are logged
 * and skipped.
 */
public class SinkRunner {

    private static final Logger LOG = LogManager.getLogger(SinkRunner.class);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    private final String           name;
    private final Sink             sink;
    private final AppProperties    appProps;
    private final Properties       sinkConfig;
    private final SinkStats        stats;
    private final List<Transform>  transforms;

    private ExecutorService             executor;
    private final AtomicBoolean         shutdown   = new AtomicBoolean(false);
    private final List<KafkaConsumer<?, ?>> consumers = new ArrayList<>();
    private KafkaProducer<String, String>   dlqProducer;
    private String                          dlqTopic;

    public SinkRunner(String name, Sink sink, AppProperties appProps, Properties sinkConfig) {
        this.name       = name;
        this.sink       = sink;
        this.appProps   = appProps;
        this.sinkConfig = sinkConfig;
        this.stats      = new SinkStats(name, sink.type());
        this.transforms = Transform.loadChain(sinkConfig.getProperty("transforms", ""));
    }

    public SinkStats getStats() { return stats; }

    /** Configure the sink, build consumers and optionally a DLQ producer, then start workers. */
    public void start() {
        sink.configure(sinkConfig);

        dlqTopic = sinkConfig.getProperty("dlq.topic");
        if (dlqTopic != null) {
            dlqProducer = KafkaClientFactory.createProducer(appProps);
            LOG.info("SinkRunner '{}': DLQ enabled -> topic={}", name, dlqTopic);
        }

        List<String> topics = Arrays.stream(
                sinkConfig.getProperty("topics", "").split(","))
            .map(String::trim)
            .filter(t -> !t.isEmpty())
            .toList();

        if (topics.isEmpty()) throw new IllegalArgumentException(
            "sink." + name + ".topics is required");

        int numThreads = Integer.parseInt(sinkConfig.getProperty("threads", "1"));
        int[] workerIdx = {0};
        executor = Executors.newFixedThreadPool(numThreads, r -> {
            Thread t = new Thread(r, "sink-" + name + "-worker-" + workerIdx[0]++);
            t.setDaemon(false);
            return t;
        });

        MessageFormat format = MessageFormat.from(appProps.get("message.format", "json"));
        submitWorkers(format, topics, numThreads);

        stats.status = ComponentStatus.RUNNING;
        LOG.info("SinkRunner '{}' started: type={} topics={} threads={} dlq={}",
            name, sink.type(), topics, numThreads, dlqTopic != null ? dlqTopic : "disabled");
    }

    /** Wake up all consumers and await orderly termination. */
    public void shutdown() {
        LOG.info("Shutting down SinkRunner '{}'", name);
        shutdown.set(true);
        consumers.forEach(KafkaConsumer::wakeup);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        try { sink.close(); } catch (Exception e) {
            LOG.warn("Error closing sink '{}': {}", name, e.getMessage());
        }
        for (Transform t : transforms) {
            try { t.close(); } catch (Exception e) {
                LOG.warn("Error closing transform '{}' in sink '{}': {}", t.type(), name, e.getMessage());
            }
        }
        stats.status = ComponentStatus.STOPPED;
        if (dlqProducer != null) {
            dlqProducer.flush();
            dlqProducer.close();
        }
        LOG.info("SinkRunner '{}' shut down", name);
    }

    // -----------------------------------------------------------------------
    // Worker wiring — typed internally, erased at the runner boundary
    // -----------------------------------------------------------------------

    @SuppressWarnings({"unchecked", "resource"})
    private void submitWorkers(MessageFormat format, List<String> topics, int numThreads) {
        switch (format) {
            case JSON -> {
                for (int i = 0; i < numThreads; i++) {
                    KafkaConsumer<String, String> c = KafkaClientFactory.createConsumer(appProps);
                    consumers.add(c);
                    executor.submit(() -> runWorker(c, Function.identity(), topics));
                }
            }
            case AVRO -> {
                // With Schema Registry the deserialiser fetches the schema from the registry
                // and returns GenericRecord directly — no local schema file needed.
                // With file-based Avro, the schema file is required.
                boolean useRegistry = appProps.get("message.schema.registry.url", null) != null;
                Schema schema = useRegistry ? null : SchemaLoader.fromFile(appProps);
                for (int i = 0; i < numThreads; i++) {
                    KafkaConsumer<String, GenericRecord> c = useRegistry
                        ? KafkaClientFactory.createSchemaRegistryConsumer(appProps)
                        : KafkaClientFactory.createAvroConsumer(appProps, schema);
                    consumers.add(c);
                    executor.submit(() -> runWorker(c, AvroConverter::toJson, topics));
                }
            }
            default -> throw new IllegalArgumentException("Unsupported message.format: " + format);
        }
    }

    /**
     * Run each record's value through the transform chain.
     * A null return from any transform drops the record (it is not passed to the sink
     * and is not routed to the DLQ — a drop is intentional, not a failure).
     * Kafka metadata (topic, partition, offset, key, timestamp) is preserved unchanged.
     */
    private List<Record> applyTransforms(List<Record> batch) {
        List<Record> out = new ArrayList<>(batch.size());
        for (Record r : batch) {
            String value = r.value();
            for (Transform t : transforms) {
                value = t.apply(value);
                if (value == null) break;
            }
            if (value != null) {
                out.add(new Record(r.topic(), r.partition(), r.offset(),
                                   r.key(), value, r.timestamp()));
            }
        }
        return out;
    }

    private <V> void runWorker(
            KafkaConsumer<String, V> consumer,
            Function<V, String> toJson,
            List<String> topics) {

        try {
            consumer.subscribe(topics);
            while (!shutdown.get()) {
                ConsumerRecords<String, V> polled = consumer.poll(POLL_TIMEOUT);
                for (TopicPartition tp : consumer.assignment()) {
                    consumer.currentLag(tp).ifPresent(lag ->
                        stats.updateLag(tp.topic() + ":" + tp.partition(), lag));
                }
                if (polled.isEmpty()) continue;

                List<Record> batch = new ArrayList<>(polled.count());
                for (ConsumerRecord<String, V> r : polled) {
                    batch.add(new Record(
                        r.topic(), r.partition(), r.offset(),
                        r.key(), toJson.apply(r.value()),
                        Instant.ofEpochMilli(r.timestamp())
                    ));
                }

                List<Record> toWrite = transforms.isEmpty() ? batch : applyTransforms(batch);
                if (toWrite.isEmpty()) {
                    consumer.commitSync();
                    continue;
                }

                List<Record> failures;
                try {
                    failures = sink.writeBatch(toWrite);
                } catch (Exception e) {
                    LOG.error("SinkRunner '{}': writeBatch threw - routing {} records: {}",
                        name, toWrite.size(), e.getMessage(), e);
                    // Whole batch failed — route all to DLQ if configured, else skip
                    failures = (dlqProducer != null) ? toWrite : List.of();
                }

                if (!failures.isEmpty() && dlqProducer != null) {
                    for (Record f : failures) {
                        dlqProducer.send(
                            new ProducerRecord<>(dlqTopic, f.key(), f.value()),
                            (meta, ex) -> {
                                if (ex != null) LOG.error("Failed to send to DLQ {}: {}", dlqTopic, ex.getMessage(), ex);
                            }
                        );
                    }
                } else if (!failures.isEmpty()) {
                    LOG.warn("SinkRunner '{}': {} records failed but no DLQ configured - skipping",
                        name, failures.size());
                }

                consumer.commitSync();
            }
        } catch (WakeupException e) {
            // Expected on shutdown — exit cleanly.
        } catch (Exception e) {
            stats.status = ComponentStatus.ERROR;
            LOG.error("SinkRunner '{}' worker crashed: {}", name, e.getMessage(), e);
        } finally {
            try { consumer.close(); } catch (Exception e) {
                LOG.warn("Error closing consumer in sink '{}': {}", name, e.getMessage());
            }
        }
    }
}
