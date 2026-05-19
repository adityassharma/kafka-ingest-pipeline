package io.github.adityassharma.kafka.sources;

import io.github.adityassharma.kafka.spi.Source;
import io.github.adityassharma.kafka.spi.SourceContext;
import io.github.adityassharma.kafka.sources.util.DataFetcher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Polling source that fetches JSON from an HTTP endpoint on a fixed interval
 * and emits each response as a single record.
 *
 * <p>Required config keys (after prefix stripping):
 * <ul>
 *   <li>{@code url}                  — HTTP endpoint to poll</li>
 *   <li>{@code topic}                — Kafka topic to emit to</li>
 *   <li>{@code polling.interval.ms}  — sleep between polls (default 5000)</li>
 * </ul>
 *
 * <p>{@link #start(SourceContext)} blocks in a polling loop until {@link #stop()} is called.
 */
public class IssApiSource implements Source {

    private static final Logger LOG = LogManager.getLogger(IssApiSource.class);

    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private DataFetcher dataFetcher;

    @Override
    public String type() {
        return "iss-api";
    }

    @Override
    public void start(SourceContext context) throws Exception {
        String url        = context.config().getProperty("url");
        String topic      = context.config().getProperty("topic");
        long   intervalMs = Long.parseLong(context.config().getProperty("polling.interval.ms", "5000"));

        if (url == null || url.isBlank())   throw new IllegalArgumentException("source url is required");
        if (topic == null || topic.isBlank()) throw new IllegalArgumentException("source topic is required");

        dataFetcher = new DataFetcher();
        LOG.info("IssApiSource started: url={} topic={} interval={}ms", url, topic, intervalMs);

        while (!stopped.get()) {
            String json = dataFetcher.fetch(url);
            if (json != null) {
                LOG.info("IssApiSource fetched record, emitting to topic '{}'", topic);
                LOG.debug("IssApiSource payload: {}", json);
                context.emit(topic, json);
            } else {
                LOG.warn("IssApiSource: fetch returned null for url={}", url);
            }
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        LOG.info("IssApiSource stopped");
    }

    @Override
    public void stop() {
        stopped.set(true);
    }

    @Override
    public void close() throws IOException {
        stop();
        if (dataFetcher != null) {
            dataFetcher.close();
        }
    }
}
