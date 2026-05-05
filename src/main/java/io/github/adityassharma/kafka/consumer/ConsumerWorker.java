package io.github.adityassharma.kafka.consumer;

import io.github.adityassharma.kafka.common.AppProperties;
import io.github.adityassharma.kafka.common.ElasticsearchSink;
import io.github.adityassharma.kafka.common.KafkaClientFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A single consumer worker thread.
 *
 * <p>Design decisions:
 * <ul>
 *   <li>Each worker owns exactly one {@link KafkaConsumer} — KafkaConsumer is
 *       NOT thread-safe and must never be shared.</li>
 *   <li>Each worker subscribes to the topic via the consumer-group protocol
 *       ({@code KafkaConsumer.subscribe()}).  The broker's group coordinator
 *       assigns partitions via rebalance, allowing dynamic scaling without
 *       restarts.</li>
 *   <li>Offsets are committed synchronously after every poll batch to avoid
 *       re-processing on restart.  For higher throughput, switch to
 *       {@code commitAsync()} with a callback.</li>
 *   <li>The worker indexes each record to Elasticsearch via
 *       {@link ElasticsearchSink}.</li>
 * </ul>
 *
 * <p>Graceful shutdown: call {@link #shutdown()}, which triggers
 * {@link KafkaConsumer#wakeup()} to interrupt the blocking {@code poll()} and
 * sets the stop flag so the loop exits cleanly.
 */
public class ConsumerWorker implements Runnable {

    private static final Logger LOG = LogManager.getLogger(ConsumerWorker.class);

    private final int workerId;
    private final String topicName;
    private final AppProperties appProps;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private KafkaConsumer<String, String> consumer;

    public ConsumerWorker(int workerId, String topicName, AppProperties appProps) {
        this.workerId  = workerId;
        this.topicName = topicName;
        this.appProps  = appProps;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("consumer-worker-" + workerId);
        LOG.info("Worker {} starting. Subscribing to topic: {}", workerId, topicName);

        consumer = KafkaClientFactory.createConsumer(appProps);
        consumer.subscribe(Collections.singletonList(topicName));

        try (ElasticsearchSink esSink = new ElasticsearchSink(appProps)) {

            Duration pollTimeout = Duration.ofMillis(
                appProps.getLong("fetch.max.wait.ms", 500));

            while (running.get()) {
                ConsumerRecords<String, String> records;
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

                for (ConsumerRecord<String, String> record : records) {
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
     * Process a single Kafka record: log it and send to Elasticsearch.
     */
    private void processRecord(ConsumerRecord<String, String> record,
                               ElasticsearchSink esSink) {
        LOG.info("Worker {} | topic={} partition={} offset={} key={} value={}",
            workerId,
            record.topic(),
            record.partition(),
            record.offset(),
            record.key(),
            record.value());

        // Use "topic-partition-offset" as document ID — guarantees idempotent
        // indexing (re-processing the same record produces the same doc ID).
        String docId = record.topic() + "-" + record.partition() + "-" + record.offset();
        esSink.index(docId, record.value());
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