package io.github.adityassharma.kafka.sinks;

import io.github.adityassharma.kafka.spi.Sink;
import io.github.adityassharma.kafka.spi.Record;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Sink that logs each record via Log4j2.  Useful for fan-out verification,
 * debugging, and as a reference Sink implementation.
 *
 * <p>Config keys (after prefix stripping):
 * <ul>
 *   <li>{@code log.level} — INFO (default), DEBUG, WARN, ERROR, TRACE</li>
 * </ul>
 */
public class LoggingSink implements Sink {

    private static final Logger LOG = LogManager.getLogger(LoggingSink.class);

    private Level logLevel;

    @Override
    public String type() {
        return "logging";
    }

    @Override
    public void configure(Properties props) {
        String levelStr = props.getProperty("log.level", "INFO");
        logLevel = Level.toLevel(levelStr, Level.INFO);
        LOG.info("LoggingSink configured with level={}", logLevel);
    }

    @Override
    public List<Record> writeBatch(List<Record> records) {
        for (Record r : records) {
            LOG.log(logLevel, "[{}] topic={} partition={} offset={} key={} value={}",
                r.timestamp(), r.topic(), r.partition(), r.offset(), r.key(), r.value());
        }
        return Collections.emptyList();
    }

    @Override
    public void close() {
        // No resources to release.
    }
}
