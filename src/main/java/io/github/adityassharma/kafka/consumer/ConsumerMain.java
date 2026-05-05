package io.github.adityassharma.kafka.consumer;

import io.github.adityassharma.kafka.common.AppProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
 * <h2>Partition Assignment Strategy</h2>
 * Each worker subscribes to the topic via the consumer-group protocol
 * ({@code KafkaConsumer.subscribe()}).  The broker's group coordinator
 * distributes partitions across workers on each rebalance using the
 * configured {@code partition.assignment.strategy} (default: RangeAssignor).
 * Assumption: {@code num.consumer.threads} evenly divides the number of
 * topic partitions.
 *
 * <h2>Shutdown</h2>
 * A JVM shutdown hook sends {@code shutdown()} to every worker and waits for
 * orderly termination (up to 30 s) before forcing exit.
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

    public static void main(String[] args) throws InterruptedException {

        // ---------------------------------------------------------------
        // 1.  Load configuration
        // ---------------------------------------------------------------
        if (args.length < 1) {
            System.err.println("Usage: ConsumerMain <path-to-consumer.properties>");
            System.exit(1);
        }

        AppProperties appProps = AppProperties.load(args[0]);

        // Publish log directory as a system property so Log4j2 picks it up
        System.setProperty("app.log.dir", appProps.get("app.log.dir", "logs"));

        String topicName = appProps.getRequired("topic.name");
        int numThreads   = appProps.getRequiredInt("num.consumer.threads");

        LOG.info("ConsumerMain starting. topic={} threads={}", topicName, numThreads);

        // ---------------------------------------------------------------
        // 2.  Create worker threads — each subscribes via group rebalance
        // ---------------------------------------------------------------
        List<ConsumerWorker> workers = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        for (int i = 0; i < numThreads; i++) {
            ConsumerWorker worker = new ConsumerWorker(i, topicName, appProps);
            workers.add(worker);
            executor.submit(worker);
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
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }
}