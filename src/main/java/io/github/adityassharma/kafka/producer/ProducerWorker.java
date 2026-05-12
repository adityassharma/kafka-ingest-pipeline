package io.github.adityassharma.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * A single producer worker thread, generic over the Kafka message value type {@code V}.
 *
 * <p>Each worker:
 * <ol>
 *   <li>Fetches JSON from one HTTP endpoint via {@link DataFetcher}.</li>
 *   <li>Converts the raw JSON string to {@code V} using the injected {@code converter}.</li>
 *   <li>Publishes the converted value as a Kafka message.</li>
 *   <li>Sleeps for {@code polling.interval.ms} before fetching again.</li>
 * </ol>
 *
 * <h2>Format wiring</h2>
 * <ul>
 *   <li>JSON: {@code V = String}, converter = {@code Function.identity()}</li>
 *   <li>Avro (file-based): {@code V = GenericRecord}, converter = {@code json -> AvroConverter.fromJson(json, schema)}</li>
 *   <li>Avro (Schema Registry): same as above — the serializer handles registry interaction</li>
 * </ul>
 *
 * <p>The converter is injected by {@link ProducerMain}, which detects {@code message.format}
 * and wires the appropriate {@link KafkaProducer} and converter.
 *
 * <p>Keying strategy: the data source URL is used as the message key so that records
 * from the same endpoint always land on the same partition.
 *
 * <p>Shutdown: call {@link #shutdown()} from any thread.
 *
 * @param <V> Kafka message value type (String for JSON, GenericRecord for Avro)
 */
public class ProducerWorker<V> implements Runnable {

    private static final Logger LOG = LogManager.getLogger(ProducerWorker.class);

    private final int workerId;
    private final String dataSourceUrl;
    private final String topicName;
    private final long pollingIntervalMs;
    private final KafkaProducer<String, V> producer;
    private final DataFetcher dataFetcher;
    private final Function<String, V> converter;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public ProducerWorker(int workerId,
                          String dataSourceUrl,
                          String topicName,
                          long pollingIntervalMs,
                          KafkaProducer<String, V> producer,
                          DataFetcher dataFetcher,
                          Function<String, V> converter) {
        this.workerId          = workerId;
        this.dataSourceUrl     = dataSourceUrl;
        this.topicName         = topicName;
        this.pollingIntervalMs = pollingIntervalMs;
        this.producer          = producer;
        this.dataFetcher       = dataFetcher;
        this.converter         = converter;
    }

    @SuppressWarnings("BusyWait") // intentional polling sleep, not a spin-wait
    @Override
    public void run() {
        Thread.currentThread().setName("producer-worker-" + workerId);
        LOG.info("Worker {} starting. url={} topic={} interval={}ms",
            workerId, dataSourceUrl, topicName, pollingIntervalMs);

        long messageCount = 0;

        while (running.get()) {
            try {
                // ---- Fetch ----
                String json = dataFetcher.fetch(dataSourceUrl);

                if (json != null && !json.isBlank()) {
                    // ---- Convert (identity for JSON, JSON→GenericRecord for Avro) ----
                    V value = converter.apply(json);

                    // ---- Publish ----
                    ProducerRecord<String, V> record =
                        new ProducerRecord<>(topicName, dataSourceUrl, value);

                    // Async send with callback. To block until the broker acknowledges
                    // (guaranteed ordering), replace with: producer.send(record).get();
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            LOG.error("Worker {} send failed: {}", workerId, exception.getMessage(), exception);
                        } else {
                            LOG.debug("Worker {} sent to {}-{} offset={}",
                                workerId, metadata.topic(), metadata.partition(), metadata.offset());
                        }
                    });

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
