package io.github.adityassharma.kafka.spi;

import java.time.Instant;

/**
 * An immutable record flowing through the pipeline — from source emit, through
 * any transform chain, to sink delivery.
 *
 * <p>The {@code value} is always a JSON string regardless of the wire format
 * (JSON or Avro) used between broker and consumer; format conversion is handled
 * by {@code SinkRunner} before this record is constructed.
 *
 * <p>On the sink side, {@code partition} and {@code offset} are the Kafka-assigned
 * values and are stable across reprocessing.  On the source side (when used by
 * source-side transforms), {@code partition} and {@code offset} are {@code -1}
 * because Kafka has not yet assigned them.
 */
public record Record(
    String  topic,
    int     partition,
    long    offset,
    String  key,
    String  value,
    Instant timestamp
) {}
