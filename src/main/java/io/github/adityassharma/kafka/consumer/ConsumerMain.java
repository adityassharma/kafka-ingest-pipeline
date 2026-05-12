package io.github.adityassharma.kafka.consumer;

import io.github.adityassharma.kafka.common.AppProperties;
import io.github.adityassharma.kafka.common.AvroConverter;
import io.github.adityassharma.kafka.common.KafkaClientFactory;
import io.github.adityassharma.kafka.common.MessageFormat;
import io.github.adityassharma.kafka.common.SchemaLoader;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Entry point for the multi-threaded Kafka consumer.
 *
 * <h2>Threading Model</h2>
 * <pre>
 *   ConsumerMain
 *       |
 *       +-- ConsumerWorker-0  (owns KafkaConsumer, subscribed via group rebalance)
 *       +-- ConsumerWorker-1  (owns KafkaConsumer, subscribed via group rebalance)
 * </pre>
 *
 * <h2>Message format</h2>
 * <p>Controlled by {@code message.format} in the properties file:
 * <ul>
 *   <li>{@code json} (default) — String value, passed to Elasticsearch as-is</li>
 *   <li>{@code avro} + {@code message.schema.file} — Avro binary, file-based schema;
 *       GenericRecord is re-encoded to JSON before Elasticsearch indexing</li>
 *   <li>{@code avro} + {@code message.schema.registry.url} — Avro with Confluent Schema Registry</li>
 * </ul>
 *
 * <h2>Usage</h2>
 * <pre>
 *   java -cp kafka-ingest-pipeline-fat.jar io.github.adityassharma.kafka.consumer.ConsumerMain config/consumer.properties
 * </pre>
 */
public class ConsumerMain {

    private static final Logger LOG = LogManager.getLogger(ConsumerMain.class);

    /** Grace period to wait for worker threads on shutdown. */
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 30;

    static void main(String[] args) throws InterruptedException {

        // ---------------------------------------------------------------
        // 1.  Load configuration
        // ---------------------------------------------------------------
        if (args.length < 1) {
            System.err.println("Usage: ConsumerMain <path-to-consumer.properties>");
            System.exit(1);
        }

        AppProperties appProps = AppProperties.load(args[0]);
        System.setProperty("app.log.dir", appProps.get("app.log.dir", "logs"));

        String        topicName  = appProps.getRequired("topic.name");
        int           numThreads = appProps.getRequiredInt("num.consumer.threads");
        MessageFormat format     = MessageFormat.from(appProps.get("message.format", "json"));

        LOG.info("ConsumerMain starting. topic={} threads={} format={}",
            topicName, numThreads, format);

        // ---------------------------------------------------------------
        // 2.  Create worker threads — supplier + toJson depend on format
        // ---------------------------------------------------------------
        ExecutorService      executor = Executors.newFixedThreadPool(numThreads);
        List<ConsumerWorker<?>> workers;

        if (format == MessageFormat.JSON) {
            workers = buildConsumerWorkers(
                appProps, topicName, numThreads,
                () -> KafkaClientFactory.createConsumer(appProps),
                Function.identity(),
                executor);

        } else {
            // Avro: choose file-based schema or Schema Registry
            String registryUrl = appProps.getRawProperties().getProperty("message.schema.registry.url");
            if (registryUrl != null && !registryUrl.isBlank()) {
                workers = buildConsumerWorkers(
                    appProps, topicName, numThreads,
                    () -> KafkaClientFactory.createSchemaRegistryConsumer(appProps),
                    AvroConverter::toJson,
                    executor);
            } else {
                Schema schema = SchemaLoader.fromFile(appProps);
                workers = buildConsumerWorkers(
                    appProps, topicName, numThreads,
                    () -> KafkaClientFactory.createAvroConsumer(appProps, schema),
                    AvroConverter::toJson,
                    executor);
            }
        }

        // ---------------------------------------------------------------
        // 3.  Register shutdown hook for graceful termination
        // ---------------------------------------------------------------
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown hook triggered. Stopping {} worker(s)...", workers.size());
            workers.forEach(ConsumerWorker::shutdown);
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
            LOG.info("ConsumerMain shutdown complete.");
        }, "shutdown-hook"));

        // Keep main thread alive — workers run until shutdown hook fires
        Thread.currentThread().join();
    }

    // -----------------------------------------------------------------------
    // Helper — typed so the compiler verifies supplier/toJson/worker alignment
    // -----------------------------------------------------------------------

    private static <V> List<ConsumerWorker<?>> buildConsumerWorkers(
            AppProperties appProps,
            String topicName,
            int numThreads,
            Supplier<KafkaConsumer<String, V>> consumerSupplier,
            Function<V, String> toJson,
            ExecutorService executor) {

        List<ConsumerWorker<?>> workers = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            ConsumerWorker<V> worker = new ConsumerWorker<>(
                i, topicName, consumerSupplier, toJson, appProps);
            workers.add(worker);
            executor.submit(worker);
        }
        return workers;
    }
}
