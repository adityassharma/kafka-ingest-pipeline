package io.github.adityassharma.kafka.spi;

import java.io.Closeable;
import java.util.List;
import java.util.Properties;

/**
 * SPI contract for pipeline data sinks.
 *
 * <p>Implementations are discovered via {@link java.util.ServiceLoader} from
 * {@code META-INF/services/io.github.adityassharma.kafka.spi.Sink}.
 * Each implementation must have a public no-arg constructor.
 *
 * <p>Lifecycle managed by {@code SinkRunner}:
 * <ol>
 *   <li>{@link #configure(Properties)} — called once before any writes.</li>
 *   <li>{@link #writeBatch(List)} — called once per Kafka poll batch.</li>
 *   <li>{@link #close()} — release resources.</li>
 * </ol>
 *
 * <p><b>Thread safety:</b> a single Sink instance is shared across all worker
 * threads for a given sink configuration.  Implementations must be thread-safe.
 *
 * <p><b>DLQ behaviour:</b> if {@code dlq.topic} is present in the props passed
 * to {@code configure()}, implementations should perform per-item failure tracking
 * and return failed records.  Without a DLQ topic, returning an empty list on
 * bulk-level success is sufficient.
 */
public interface Sink extends Closeable {

    /** Unique type identifier matching {@code sink.<name>.type} in pipeline.properties. */
    String type();

    /**
     * Configure the sink.  Props are scoped to this sink instance
     * (prefix stripped — keys are plain names like {@code elasticsearch.host}).
     */
    void configure(Properties props);

    /**
     * Write a batch of records to the destination.
     *
     * @param records the full batch from a single Kafka {@code poll()} call
     * @return records that could not be written (empty list = all succeeded).
     *         When {@code dlq.topic} is configured, failed items are returned
     *         individually; otherwise an empty list is acceptable on full-batch failure
     *         (the runner logs and skips in that case).
     * @throws Exception if the entire batch fails catastrophically
     */
    List<SinkRecord> writeBatch(List<SinkRecord> records) throws Exception;
}
