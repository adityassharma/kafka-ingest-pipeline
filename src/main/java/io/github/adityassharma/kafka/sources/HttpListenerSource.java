package io.github.adityassharma.kafka.sources;

import com.sun.net.httpserver.HttpServer;
import io.github.adityassharma.kafka.spi.Source;
import io.github.adityassharma.kafka.spi.SourceContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/**
 * Listener source that accepts HTTP POST requests and emits each request body
 * as a record.  Designed for push-based ingestion from agents such as FluentBit.
 *
 * <p>Required config keys (after prefix stripping):
 * <ul>
 *   <li>{@code topic}    — Kafka topic to emit to</li>
 *   <li>{@code port}     — TCP port to listen on (default 8080)</li>
 *   <li>{@code path}     — URL path to accept POSTs on (default /ingest)</li>
 *   <li>{@code threads}  — HTTP handler thread pool size (default 4)</li>
 * </ul>
 *
 * <p>{@link #start(SourceContext)} starts the server and returns immediately;
 * the server runs on its own thread pool until {@link #stop()} is called.
 */
public class HttpListenerSource implements Source {

    private static final Logger LOG = LogManager.getLogger(HttpListenerSource.class);

    private HttpServer server;

    @Override
    public String type() {
        return "http-listener";
    }

    @Override
    public void start(SourceContext context) throws IOException {
        String topic   = context.config().getProperty("topic");
        int    port    = Integer.parseInt(context.config().getProperty("port",    "8080"));
        String path    = context.config().getProperty("path",    "/ingest");
        int    threads = Integer.parseInt(context.config().getProperty("threads", "4"));

        if (topic == null || topic.isBlank()) throw new IllegalArgumentException("source topic is required");

        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(threads));

        server.createContext(path, exchange -> {
            try {
                if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
                byte[] bodyBytes = exchange.getRequestBody().readAllBytes();
                String json = new String(bodyBytes, StandardCharsets.UTF_8);
                context.emit(topic, json);
                exchange.sendResponseHeaders(204, -1);
            } catch (Exception e) {
                LOG.error("Error handling ingest request: {}", e.getMessage());
                exchange.sendResponseHeaders(500, -1);
            } finally {
                exchange.close();
            }
        });

        server.start();
        LOG.info("HttpListenerSource started on port={} path={} threads={}", port, path, threads);
        // Returns immediately; server runs on its own executor threads.
    }

    @Override
    public void stop() {
        if (server != null) {
            server.stop(1);
            LOG.info("HttpListenerSource stopped");
        }
    }

    @Override
    public void close() {
        stop();
    }
}
