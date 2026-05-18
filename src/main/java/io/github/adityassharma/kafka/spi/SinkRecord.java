package io.github.adityassharma.kafka.spi;

import java.time.Instant;

/**
 * An immutable record delivered to a {@link Sink} for processing.
 * The {@code value} is always a JSON string, regardless of the wire format
 * used between Kafka broker and consumer — conversion is handled by SinkRunner.
 */
public record SinkRecord(
    String topic,
    int    partition,
    long   offset,
    String key,
    String value,
    Instant timestamp
) {}
