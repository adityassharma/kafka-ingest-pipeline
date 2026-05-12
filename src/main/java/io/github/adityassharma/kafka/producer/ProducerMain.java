package io.github.adityassharma.kafka.producer;

import io.github.adityassharma.kafka.common.AppProperties;
import io.github.adityassharma.kafka.common.AvroConverter;
import io.github.adityassharma.kafka.common.KafkaClientFactory;
import io.github.adityassharma.kafka.common.MessageFormat;
import io.github.adityassharma.kafka.common.SchemaLoader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Entry point for the multi-threaded Kafka producer.
 *
 * <h2>Threading Model</h2>
 * <pre>
 *   ProducerMain
 *       |
 *       +-- [shared] KafkaProducer  (thread-safe, one per JVM)
 *       +-- [shared] DataFetcher    (thread-safe HTTP client)
 *       |
 *       +-- ProducerWorker-0  (polls ISS position API)
 * </pre>
 *
 * <p>Because {@link KafkaProducer} is thread-safe, it is more efficient to
 * share a single instance across workers (shared send buffer, one TCP
 * connection per broker).
 *
 * <h2>Message format</h2>
 * <p>Controlled by {@code message.format} in the properties file:
 * <ul>
 *   <li>{@code json} (default) — raw JSON string, no schema required</li>
 *   <li>{@code avro} + {@code message.schema.file} — Avro binary, file-based schema</li>
 *   <li>{@code avro} + {@code message.schema.registry.url} — Avro with Confluent Schema Registry</li>
 * </ul>
 *
 * <h2>Usage</h2>
 * <pre>
 *   java -cp kafka-ingest-pipeline-fat.jar io.github.adityassharma.kafka.producer.ProducerMain config/producer.properties
 * </pre>
 */
public class ProducerMain {

    private static final Logger LOG = LogManager.getLogger(ProducerMain.class);
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 15;

    /** Data source URL property keys, assigned to workers round-robin. */
    private static final String[] DATA_SOURCE_KEYS = {
        "data.source.iss.position"
    };

    static void main(String[] args) throws InterruptedException {

        // ---------------------------------------------------------------
        // 1.  Load configuration
        // ---------------------------------------------------------------
        if (args.length < 1) {
            System.err.println("Usage: ProducerMain <path-to-producer.properties>");
            System.exit(1);
        }

        AppProperties appProps = AppProperties.load(args[0]);
        System.setProperty("app.log.dir", appProps.get("app.log.dir", "logs"));

        String        topicName       = appProps.getRequired("topic.name");
        int           numThreads      = appProps.getRequiredInt("num.producer.threads");
        long          pollingInterval = appProps.getLong("polling.interval.ms", 5000);
        MessageFormat format          = MessageFormat.from(appProps.get("message.format", "json"));

        LOG.info("ProducerMain starting. topic={} threads={} interval={}ms format={}",
            topicName, numThreads, pollingInterval, format);

        // ---------------------------------------------------------------
        // 2.  Build shared resources — producer type depends on format
        // ---------------------------------------------------------------
        DataFetcher       sharedFetcher  = new DataFetcher();
        ExecutorService   executor       = Executors.newFixedThreadPool(numThreads);
        List<ProducerWorker<?>> workers  = new ArrayList<>();
        KafkaProducer<?, ?>     sharedProducer;

        if (format == MessageFormat.JSON) {
            KafkaProducer<String, String> p = KafkaClientFactory.createProducer(appProps);
            sharedProducer = p;
            buildAndStartWorkers(appProps, topicName, numThreads, pollingInterval,
                p, sharedFetcher, Function.identity(), workers, executor);

        } else {
            // Avro: choose file-based schema or Schema Registry
            String registryUrl = appProps.getRawProperties().getProperty("message.schema.registry.url");
            if (registryUrl != null && !registryUrl.isBlank()) {
                KafkaProducer<String, GenericRecord> p =
                    KafkaClientFactory.createSchemaRegistryProducer(appProps);
                sharedProducer = p;
                // Schema is managed by the registry; converter parses ISS JSON to GenericRecord
                // using the schema pre-registered under the topic's subject.
                Schema schema = SchemaLoader.fromFile(appProps);
                buildAndStartWorkers(appProps, topicName, numThreads, pollingInterval,
                    p, sharedFetcher, json -> AvroConverter.fromJson(json, schema), workers, executor);
            } else {
                Schema schema = SchemaLoader.fromFile(appProps);
                KafkaProducer<String, GenericRecord> p =
                    KafkaClientFactory.createAvroProducer(appProps, schema);
                sharedProducer = p;
                buildAndStartWorkers(appProps, topicName, numThreads, pollingInterval,
                    p, sharedFetcher, json -> AvroConverter.fromJson(json, schema), workers, executor);
            }
        }

        LOG.info("All {} producer worker(s) started.", numThreads);

        // ---------------------------------------------------------------
        // 3.  Shutdown hook
        // ---------------------------------------------------------------
        final KafkaProducer<?, ?> producerRef = sharedProducer;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown hook triggered. Stopping {} producer worker(s)...", workers.size());

            workers.forEach(ProducerWorker::shutdown);
            executor.shutdown();

            try {
                if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    LOG.warn("Workers did not stop within {}s; forcing shutdown.",
                        SHUTDOWN_TIMEOUT_SECONDS);
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
            }

            try {
                producerRef.flush();
                producerRef.close();
                LOG.info("KafkaProducer closed.");
            } catch (Exception e) {
                LOG.warn("Error closing KafkaProducer: {}", e.getMessage());
            }

            try {
                sharedFetcher.close();
            } catch (Exception e) {
                LOG.warn("Error closing DataFetcher: {}", e.getMessage());
            }

            LOG.info("ProducerMain shutdown complete.");
        }, "shutdown-hook"));

        // ---------------------------------------------------------------
        // 4.  Keep main thread alive
        // ---------------------------------------------------------------
        Thread.currentThread().join();
    }

    // -----------------------------------------------------------------------
    // Helper — typed so the compiler verifies producer/converter/worker alignment
    // -----------------------------------------------------------------------

    private static <V> void buildAndStartWorkers(
            AppProperties appProps,
            String topicName,
            int numThreads,
            long pollingIntervalMs,
            KafkaProducer<String, V> producer,
            DataFetcher fetcher,
            Function<String, V> converter,
            List<ProducerWorker<?>> workers,
            ExecutorService executor) {

        for (int i = 0; i < numThreads; i++) {
            String urlKey = DATA_SOURCE_KEYS[i % DATA_SOURCE_KEYS.length];
            String url    = appProps.getRequired(urlKey);
            ProducerWorker<V> worker = new ProducerWorker<>(
                i, url, topicName, pollingIntervalMs, producer, fetcher, converter);
            workers.add(worker);
            executor.submit(worker);
        }
    }
}
