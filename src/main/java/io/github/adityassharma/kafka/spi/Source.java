package io.github.adityassharma.kafka.spi;

import java.io.Closeable;

/**
 * SPI contract for pipeline data sources.
 *
 * <p>Implementations are discovered via {@link java.util.ServiceLoader} from
 * {@code META-INF/services/io.github.adityassharma.kafka.spi.Source}.
 * Each implementation must have a public no-arg constructor.
 *
 * <p>Lifecycle managed by {@code SourceRunner}:
 * <ol>
 *   <li>{@link #start(SourceContext)} — begin producing; may block (polling loop)
 *       or return immediately (listener-based).</li>
 *   <li>{@link #stop()} — signal graceful shutdown (non-blocking).</li>
 *   <li>{@link #close()} — release resources.</li>
 * </ol>
 */
public interface Source extends Closeable {

    /** Unique type identifier matching {@code source.<name>.type} in pipeline.properties. */
    String type();

    /**
     * Start producing records.  May block until {@link #stop()} is called
     * (polling sources) or return immediately (listener-based sources).
     */
    void start(SourceContext context) throws Exception;

    /** Signal this source to stop.  Non-blocking; {@link #close()} follows shortly after. */
    void stop();
}
