package io.github.adityassharma.kafka.common;

/**
 * Supported Kafka message formats.
 *
 * <p>Set via {@code message.format} in producer/consumer properties:
 * <ul>
 *   <li>{@code json} — raw JSON string (default); no schema required.</li>
 *   <li>{@code avro} — Avro binary encoding; requires either
 *       {@code message.schema.file} (file-based, no registry) or
 *       {@code message.schema.registry.url} (Confluent Schema Registry).</li>
 * </ul>
 */
public enum MessageFormat {

    JSON, AVRO;

    public static MessageFormat from(String value) {
        if (value == null || value.isBlank()) return JSON;
        return switch (value.trim().toLowerCase()) {
            case "json" -> JSON;
            case "avro" -> AVRO;
            default -> throw new IllegalArgumentException(
                "Unknown message.format '" + value + "'. Valid values: json, avro");
        };
    }
}
