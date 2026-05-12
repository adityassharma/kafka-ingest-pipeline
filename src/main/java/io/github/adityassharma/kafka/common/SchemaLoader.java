package io.github.adityassharma.kafka.common;

import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;

/**
 * Loads an Avro {@link Schema} from a {@code .avsc} file.
 *
 * <p>The path is read from {@code message.schema.file} in the properties file.
 */
public final class SchemaLoader {

    private static final Logger LOG = LogManager.getLogger(SchemaLoader.class);

    private SchemaLoader() {}

    /**
     * Load an Avro schema from the file path given by {@code message.schema.file}.
     *
     * @throws IllegalArgumentException if the property is absent
     * @throws RuntimeException         if the file cannot be found or parsed
     */
    public static Schema fromFile(AppProperties appProps) {
        String path = appProps.getRequired("message.schema.file");
        try {
            Schema schema = new Schema.Parser().parse(new File(path));
            LOG.info("Loaded Avro schema '{}' from {}", schema.getFullName(), path);
            return schema;
        } catch (IOException e) {
            throw new RuntimeException("Cannot load Avro schema from: " + path, e);
        }
    }
}
