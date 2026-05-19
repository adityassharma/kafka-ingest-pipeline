package io.github.adityassharma.kafka.transforms;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.adityassharma.kafka.spi.Transform;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;

/**
 * SMT that reads the payload's own {@code timestamp} field (Unix epoch seconds,
 * as present in ISS API and similar feeds) and adds a {@code recordTimestamp}
 * field containing the same instant as an ISO-8601 string.
 *
 * <p>Example input:
 * <pre>
 *   { "timestamp": 1746000000, "iss_position": { "latitude": "51.5", "longitude": "-0.1" } }
 * </pre>
 * Example output:
 * <pre>
 *   { "timestamp": 1746000000, "iss_position": { ... }, "recordTimestamp": "2025-04-30T12:00:00Z" }
 * </pre>
 *
 * <p>If the payload does not have a numeric {@code timestamp} field, or JSON
 * parsing fails, the record is forwarded unchanged (never dropped).
 *
 * <p><b>Thread-safe:</b> stateless; {@link ObjectMapper} is thread-safe after
 * construction.
 *
 * <p>Register in {@code META-INF/services/io.github.adityassharma.kafka.spi.Transform}.
 * Configure in pipeline.properties:
 * <pre>
 *   source.iss-position.transforms=inject-record-timestamp
 *   sink.es-iss.transforms=inject-record-timestamp
 * </pre>
 */
public class InjectRecordTimestamp implements Transform {

    private static final Logger LOG = LogManager.getLogger(InjectRecordTimestamp.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String type() {
        return "inject-record-timestamp";
    }

    @Override
    public String apply(String json) {
        try {
            ObjectNode node = (ObjectNode) MAPPER.readTree(json);
            if (node.has("timestamp") && node.get("timestamp").isNumber()) {
                long epochSeconds = node.get("timestamp").asLong();
                node.put("recordTimestamp", Instant.ofEpochSecond(epochSeconds).toString());
            }
            return MAPPER.writeValueAsString(node);
        } catch (Exception e) {
            LOG.warn("InjectRecordTimestamp: failed to process payload, forwarding unchanged: {}",
                e.getMessage());
            return json;
        }
    }

    @Override
    public void close() {
        // Stateless — nothing to release.
    }
}
