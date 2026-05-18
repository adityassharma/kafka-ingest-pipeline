package io.github.adityassharma.kafka.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Utility for loading and accessing application properties from an external
 * .properties file supplied as a command-line argument.
 *
 * <p>Usage:
 * <pre>
 *   AppProperties props = AppProperties.load("/path/to/consumer.properties");
 *   String brokers = props.getRequired("bootstrap.servers");
 *   int threads    = props.getInt("num.consumer.threads", 1);
 * </pre>
 */
public final class AppProperties {

    private static final Logger LOG = LogManager.getLogger(AppProperties.class);

    private final Properties props;
    private final String filePath;

    private AppProperties(Properties props, String filePath) {
        this.props = props;
        this.filePath = filePath;
    }

    /**
     * Create an AppProperties instance from an already-assembled Properties object.
     * Used by PipelineMain to build scoped views (global + prefix-stripped instance props).
     */
    public static AppProperties fromProperties(Properties props, String label) {
        return new AppProperties(props, label);
    }

    /**
     * Load properties from the given file path.
     *
     * @param filePath absolute or relative path to the .properties file
     * @return loaded AppProperties instance
     * @throws RuntimeException if the file cannot be read
     */
    public static AppProperties load(String filePath) {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(filePath)) {
            props.load(fis);
            LOG.info("Loaded properties from: {}", filePath);
        } catch (IOException e) {
            throw new RuntimeException("Cannot load properties file: " + filePath, e);
        }
        return new AppProperties(props, filePath);
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /** Returns all properties (e.g. to pass directly to KafkaConsumer). */
    public Properties getRawProperties() {
        return props;
    }

    /**
     * Get a required property; throws if absent or blank.
     */
    public String getRequired(String key) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                "Required property '" + key + "' is missing in " + filePath);
        }
        return value.trim();
    }

    /**
     * Get an optional string property with a default.
     */
    public String get(String key, String defaultValue) {
        return props.getProperty(key, defaultValue).trim();
    }

    /**
     * Get a required integer property.
     */
    public int getRequiredInt(String key) {
        return Integer.parseInt(getRequired(key));
    }

    /**
     * Get an optional integer property with a default.
     */
    public int getInt(String key, int defaultValue) {
        String val = props.getProperty(key);
        return (val == null || val.isBlank()) ? defaultValue : Integer.parseInt(val.trim());
    }

    /**
     * Get an optional long property with a default.
     */
    public long getLong(String key, long defaultValue) {
        String val = props.getProperty(key);
        return (val == null || val.isBlank()) ? defaultValue : Long.parseLong(val.trim());
    }

    /**
     * Get a required boolean property.
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        String val = props.getProperty(key);
        return (val == null || val.isBlank()) ? defaultValue : Boolean.parseBoolean(val.trim());
    }

    /**
     * Build a subset of properties whose keys start with the given prefix,
     * stripping the prefix from the returned keys.
     * Useful for extracting Kafka-native properties (e.g. "fetch.min.bytes").
     */
    public Properties subset(String... keys) {
        Properties sub = new Properties();
        for (String key : keys) {
            String val = props.getProperty(key);
            if (val != null) sub.setProperty(key, val.trim());
        }
        return sub;
    }

    @Override
    public String toString() {
        return "AppProperties{file='" + filePath + "', keys=" + props.stringPropertyNames() + "}";
    }
}
