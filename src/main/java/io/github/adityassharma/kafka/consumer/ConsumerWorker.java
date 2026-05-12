package io.github.adityassharma.kafka.consumer;

import io.github.adityassharma.kafka.common.AppProperties;
import io.github.adityassharma.kafka.common.ElasticsearchSink;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A single consumer worker thread, generic over the Kafka message value type {@code V}.
 *
 * <h2>Design decisions</h2>
 * <ul>
 *   <li>Each worker owns exactly one {@link KafkaConsumer} — KafkaConsumer is NOT
 *       thread-safe and must never be shared.  The consumer is created lazily inside
 *       {@link #run()} via the injected {@code consumerSupplier}.</li>
 *   <li>Each worker subscribes to the topic via the consumer-group protocol
 *       ({@code KafkaConsumer.subscribe()}).  The broker's group coordinator assigns
 *       partitions via rebalance, allowing dynamic scaling without restarts.</li>
 *   <li>Before indexing to Elasticsearch, the value {@code V} is converted to a JSON
 *       string by the injected {@code toJson} function.
 *       {@link ElasticsearchSink} always receives a plain JSON string.</li>
 *   <li>Offsets are committed synchronously after every poll batch.</li>
 * </ul>
 *
 * <h2>Format wiring</h2>
 * <ul>
 *   <li>JSON: {@code V = String}, toJson = {@code Function.identity()}</li>
 *   <li>Avro: {@code V = GenericRecord}, toJson = {@code AvroConverter::toJson}</li>
 * </ul>
 *
 * <p>Wiring is done by {@link ConsumerMain}, which detects {@code message.format} and
 * injects the appropriate supplier and toJson function.
 *
 * <p>Graceful shutdown: call {@link #shutdown()}, which triggers
 * {@link KafkaConsumer#wakeup()} to interrupt the blocking {@code poll()}.
 *
 * @param <V> Kafka message value type (String for JSON, GenericRecord for Avro)
 */
public class ConsumerWorker<V> implements Runnable {

    private static final Logger LOG = LogManager.getLogger(ConsumerWorker.class);

    private final int workerId;
    private final String topicName;
    private final Supplier<KafkaConsumer<String, V>> consumerSupplier;
    private final Function<V, String> toJson;
    private final AppProperties appProps;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private KafkaConsumer<String, V> consumer;

    public ConsumerWorker(int workerId,
                          String topicName,
                          Supplier<KafkaConsumer<String, V>> consumerSupplier,
                          Function<V, String> toJson,
                          AppProperties appProps) {
        this.workerId         = workerId;
        this.topicName        = topicName;
        this.consumerSupplier = consumerSupplier;
        this.toJson           = toJson;
        this.appProps         = appProps;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("consumer-worker-" + workerId);
        LOG.info("Worker {} starting. Subscribing to topic: {}", workerId, topicName);

        consumer = consumerSupplier.get();
        consumer.subscribe(Collections.singletonList(topicName));

        try (ElasticsearchSink esSink = new ElasticsearchSink(appProps)) {

            Duration pollTimeout = Duration.ofMillis(
                appProps.getLong("fetch.max.wait.ms", 500));

            while (running.get()) {
                ConsumerRecords<String, V> records;
                try {
                    records = consumer.poll(pollTimeout);
                } catch (WakeupException e) {
                    // Expected on shutdown — exit loop
                    LOG.info("Worker {} received wakeup signal, shutting down.", workerId);
                    break;
                }

                if (records.isEmpty()) {
                    continue;
                }

                LOG.debug("Worker {} polled {} records.", workerId, records.count());

                for (ConsumerRecord<String, V> record : records) {
                    processRecord(record, esSink);
                }

                // Synchronous commit after processing the entire batch.
                // If the JVM dies before this point, the batch will be re-processed
                // (at-least-once delivery guarantee).
                consumer.commitSync();
            }

        } catch (Exception e) {
            LOG.error("Worker {} encountered fatal error: {}", workerId, e.getMessage(), e);
        } finally {
            safeClose();
            LOG.info("Worker {} stopped.", workerId);
        }
    }

    /**
     * Process a single Kafka record: convert value to JSON, log it, and send to Elasticsearch.
     */
    private void processRecord(ConsumerRecord<String, V> record, ElasticsearchSink esSink) {
        String jsonValue = toJson.apply(record.value());

        LOG.info("Worker {} | topic={} partition={} offset={} key={} value={}",
            workerId,
            record.topic(),
            record.partition(),
            record.offset(),
            record.key(),
            jsonValue);

        // Use "topic-partition-offset" as document ID — guarantees idempotent
        // indexing (re-processing the same record produces the same doc ID).
        String docId = record.topic() + "-" + record.partition() + "-" + record.offset();
        esSink.index(docId, jsonValue);
    }

    /**
     * Signal this worker to stop gracefully.
     * Safe to call from any thread.
     */
    public void shutdown() {
        LOG.info("Shutdown requested for worker {}.", workerId);
        running.set(false);
        if (consumer != null) {
            consumer.wakeup(); // interrupts blocking poll()
        }
    }

    private void safeClose() {
        try {
            if (consumer != null) {
                consumer.close();
            }
        } catch (Exception e) {
            LOG.warn("Worker {} error closing consumer: {}", workerId, e.getMessage());
        }
    }
}
