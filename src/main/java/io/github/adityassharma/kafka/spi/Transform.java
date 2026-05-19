package io.github.adityassharma.kafka.spi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

/**
 * SPI contract for single-message transforms (SMTs).
 *
 * <p>Implementations are discovered via {@link ServiceLoader} from
 * {@code META-INF/services/io.github.adityassharma.kafka.spi.Transform}.
 * Each implementation must have a public no-arg constructor.
 *
 * <p>Transforms are applied in declaration order to the JSON value of a record —
 * before a source produces to Kafka (source-side) or before a sink writes to its
 * destination (sink-side).  Only the JSON payload is visible to a transform; Kafka
 * metadata (topic, partition, offset, timestamp) is not exposed.
 *
 * <p>Configure via a comma-separated list in properties files:
 * <pre>
 *   # source-side — applied before the record is produced to Kafka
 *   source.iss-position.transforms=inject-record-timestamp
 *
 *   # sink-side — applied after consuming from Kafka, before writeBatch()
 *   sink.es-iss.transforms=inject-record-timestamp
 *
 *   # chained — executed left to right; a null return drops the record
 *   sink.es-iss.transforms=inject-record-timestamp,mask-pii
 * </pre>
 *
 * <p><b>Thread safety:</b> a single Transform instance is shared across all
 * worker threads.  Implementations must be thread-safe.  Stateless implementations
 * are inherently safe.
 */
public interface Transform {

    /** Discovery key — must match an entry in the {@code transforms} list. */
    String type();

    /**
     * Apply this transform to one JSON payload.
     *
     * @param json the record value as a UTF-8 JSON string
     * @return the transformed JSON string, or {@code null} to drop the record entirely
     */
    String apply(String json);

    /**
     * Release any resources held by this transform.
     * No-op for stateless transforms; override when the implementation
     * holds connections, file handles, or thread pools.
     */
    void close();

    // -----------------------------------------------------------------------
    // Static factory
    // -----------------------------------------------------------------------

    /**
     * Parse a comma-separated list of transform type names, discover each via
     * {@link ServiceLoader}, and return the ordered chain ready for use.
     *
     * <p>Each call to this method creates fresh instances — safe to call multiple
     * times (e.g. once per source runner and once per sink runner).
     *
     * @param csv comma-separated type names, e.g. {@code "inject-record-timestamp,mask-pii"}
     * @return immutable ordered list; empty if {@code csv} is blank or null
     * @throws IllegalArgumentException if any type name has no registered implementation
     */
    static List<Transform> loadChain(String csv) {
        if (csv == null || csv.isBlank()) return List.of();

        List<String> types = Arrays.stream(csv.split(","))
            .map(String::trim)
            .filter(s -> !s.isBlank())
            .toList();

        if (types.isEmpty()) return List.of();

        List<Transform> chain = new ArrayList<>(types.size());
        for (String type : types) {
            // A new ServiceLoader is created per type so that the same type appearing
            // twice in the chain receives two independent instances.
            Transform found = null;
            for (Transform t : ServiceLoader.load(Transform.class)) {
                if (t.type().equals(type)) {
                    found = t;
                    break;
                }
            }
            if (found == null) throw new IllegalArgumentException(
                "No Transform implementation found for type='" + type +
                "'. Register it in META-INF/services/" + Transform.class.getName());
            chain.add(found);
        }
        return Collections.unmodifiableList(chain);
    }
}
