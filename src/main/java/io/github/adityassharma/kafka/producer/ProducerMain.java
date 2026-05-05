package io.github.adityassharma.kafka.producer;

import io.github.adityassharma.kafka.common.AppProperties;
import io.github.adityassharma.kafka.common.KafkaClientFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
 *       +-- ProducerWorker-1  (polls People in Space API)
 * </pre>
 *
 * <p>Because {@link KafkaProducer} is thread-safe, it is more efficient to
 * share a single instance across workers (shared send buffer, one TCP
 * connection per broker).
 *
 * <h2>Data Sources</h2>
 * <ul>
 *   <li>Thread 0 → {@code data.source.iss.position} (ISS lat/lon, ~5 s interval)</li>
 *   <li>Thread 1 → {@code data.source.people.in.space} (crew list, 5 s interval)</li>
 *   <li>Additional threads reuse existing URLs round-robin (easy to extend)</li>
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

    /** URLs in priority order — worker i gets dataSources[i % dataSources.length]. */
    private static final String[] DATA_SOURCE_KEYS = {
        "data.source.iss.position",
        "data.source.people.in.space"
    };

    public static void main(String[] args) throws InterruptedException {

        // ---------------------------------------------------------------
        // 1.  Load configuration
        // ---------------------------------------------------------------
        if (args.length < 1) {
            System.err.println("Usage: ProducerMain <path-to-producer.properties>");
            System.exit(1);
        }

        AppProperties appProps = AppProperties.load(args[0]);
        System.setProperty("app.log.dir", appProps.get("app.log.dir", "logs"));

        String topicName       = appProps.getRequired("topic.name");
        int    numThreads      = appProps.getRequiredInt("num.producer.threads");
        long   pollingInterval = appProps.getLong("polling.interval.ms", 5000);

        LOG.info("ProducerMain starting. topic={} threads={} interval={}ms",
            topicName, numThreads, pollingInterval);

        // ---------------------------------------------------------------
        // 2.  Build shared resources
        // ---------------------------------------------------------------
        KafkaProducer<String, String> sharedProducer =
            KafkaClientFactory.createProducer(appProps);
        DataFetcher sharedFetcher = new DataFetcher();

        // ---------------------------------------------------------------
        // 3.  Create and start worker threads
        // ---------------------------------------------------------------
        List<ProducerWorker> workers = new ArrayList<>();
        ExecutorService executor     = Executors.newFixedThreadPool(numThreads);

        for (int i = 0; i < numThreads; i++) {
            // Assign a URL; cycle if more threads than URLs
            String urlKey = DATA_SOURCE_KEYS[i % DATA_SOURCE_KEYS.length];
            String url    = appProps.getRequired(urlKey);

            ProducerWorker worker = new ProducerWorker(
                i, url, topicName, pollingInterval, sharedProducer, sharedFetcher);
            workers.add(worker);
            executor.submit(worker);
        }

        LOG.info("All {} producer worker(s) started.", numThreads);

        // ---------------------------------------------------------------
        // 4.  Shutdown hook
        // ---------------------------------------------------------------
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

            // Flush and close shared Kafka producer
            try {
                sharedProducer.flush();
                sharedProducer.close();
                LOG.info("KafkaProducer closed.");
            } catch (Exception e) {
                LOG.warn("Error closing KafkaProducer: {}", e.getMessage());
            }

            // Close HTTP client
            try {
                sharedFetcher.close();
            } catch (Exception e) {
                LOG.warn("Error closing DataFetcher: {}", e.getMessage());
            }

            LOG.info("ProducerMain shutdown complete.");
        }, "shutdown-hook"));

        // ---------------------------------------------------------------
        // 5.  Keep main thread alive
        // ---------------------------------------------------------------
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }
}
