package io.github.adityassharma.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A single producer worker thread.
 *
 * <p>Each worker:
 * <ol>
 *   <li>Fetches JSON from one HTTP endpoint via {@link DataFetcher}.</li>
 *   <li>Publishes the raw JSON as a Kafka message value.</li>
 *   <li>Sleeps for {@code polling.interval.ms} before fetching again.</li>
 * </ol>
 *
 * <p>Keying strategy: the URL is used as the message key.  Kafka uses a hash
 * of the key to decide which partition to route the message to, so all records
 * from the same endpoint land on the same partition — convenient for ordering.
 *
 * <p>The shared {@link KafkaProducer} is thread-safe and is passed in from
 * {@link ProducerMain}.  Sharing one producer across threads is more efficient
 * than one per thread (shared send buffer, fewer connections to the broker).
 *
 * <p>Shutdown: call {@link #shutdown()} from any thread.
 */
public class ProducerWorker implements Runnable {

    private static final Logger LOG = LogManager.getLogger(ProducerWorker.class);

    private final int workerId;
    private final String dataSourceUrl;
    private final String topicName;
    private final long pollingIntervalMs;
    private final KafkaProducer<String, String> producer;
    private final DataFetcher dataFetcher;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public ProducerWorker(int workerId,
                          String dataSourceUrl,
                          String topicName,
                          long pollingIntervalMs,
                          KafkaProducer<String, String> producer,
                          DataFetcher dataFetcher) {
        this.workerId         = workerId;
        this.dataSourceUrl    = dataSourceUrl;
        this.topicName        = topicName;
        this.pollingIntervalMs = pollingIntervalMs;
        this.producer         = producer;
        this.dataFetcher      = dataFetcher;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("producer-worker-" + workerId);
        LOG.info("Worker {} starting. url={} topic={} interval={}ms",
            workerId, dataSourceUrl, topicName, pollingIntervalMs);

        long messageCount = 0;

        while (running.get()) {
            try {
                // ---- Fetch data ----
                String json = dataFetcher.fetch(dataSourceUrl);

                if (json != null && !json.isBlank()) {
                    // ---- Publish to Kafka ----
                    ProducerRecord<String, String> record =
                        new ProducerRecord<>(topicName, dataSourceUrl, json);

                    // send() is async — we get a Future back.
                    // We call get() to block until the broker acknowledges,
                    // which gives us a simple error-handling path.
                    // For higher throughput, remove .get() and use a callback instead.
                    Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            LOG.error("Worker {} send failed: {}", workerId, exception.getMessage(), exception);
                        } else {
                            LOG.debug("Worker {} sent to {}-{} offset={}",
                                workerId, metadata.topic(), metadata.partition(), metadata.offset());
                        }
                    });

                    // Optional blocking get — uncomment if you want guaranteed ordering:
                    // future.get();

                    messageCount++;
                    LOG.info("Worker {} published message #{} from {}", workerId, messageCount, dataSourceUrl);
                } else {
                    LOG.warn("Worker {} received empty response from {}", workerId, dataSourceUrl);
                }

            } catch (Exception e) {
                LOG.error("Worker {} error during fetch/publish: {}", workerId, e.getMessage(), e);
                // Continue — transient errors (network blip, broker restart) should not kill the thread
            }

            // ---- Sleep until next poll ----
            if (running.get()) {
                try {
                    Thread.sleep(pollingIntervalMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        LOG.info("Worker {} stopped after publishing {} message(s).", workerId, messageCount);
    }

    /**
     * Signal this worker to stop after the current iteration completes.
     */
    public void shutdown() {
        LOG.info("Shutdown requested for producer worker {}.", workerId);
        running.set(false);
    }
}
