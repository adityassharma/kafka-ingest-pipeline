package io.github.adityassharma.kafka.management;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Lightweight HTTP server (JDK built-in) exposing health and metrics endpoints.
 * Created only when {@code management.port} is set in pipeline properties.
 *
 * <p>GET /health  — JSON component status for all sources and sinks.
 * <p>GET /metrics — Prometheus plaintext consumer lag gauges.
 */
public class ManagementServer {

    private static final Logger LOG = LogManager.getLogger(ManagementServer.class);

    private final int             port;
    private final List<SourceStats> sourceStats;
    private final List<SinkStats>   sinkStats;
    private HttpServer server;

    public ManagementServer(int port, List<SourceStats> sourceStats, List<SinkStats> sinkStats) {
        this.port        = port;
        this.sourceStats = sourceStats;
        this.sinkStats   = sinkStats;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "mgmt-http");
            t.setDaemon(true);
            return t;
        }));
        server.createContext("/health",  ex -> handle(ex, this::buildHealthJson,  "application/json"));
        server.createContext("/metrics", ex -> handle(ex, this::buildMetricsText, "text/plain; version=0.0.4"));
        server.start();
        LOG.info("ManagementServer started on port {}", port);
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
            LOG.info("ManagementServer stopped");
        }
    }

    // -----------------------------------------------------------------------

    @FunctionalInterface
    private interface BodySupplier { String get(); }

    private static void handle(HttpExchange ex, BodySupplier body, String contentType) throws IOException {
        if (!"GET".equals(ex.getRequestMethod())) {
            ex.sendResponseHeaders(405, -1);
            ex.close();
            return;
        }
        byte[] bytes = body.get().getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", contentType);
        ex.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    private String buildHealthJson() {
        boolean anyError   = sourceStats.stream().anyMatch(s -> s.status == ComponentStatus.ERROR)
                          || sinkStats.stream().anyMatch(s -> s.status == ComponentStatus.ERROR);
        boolean allRunning = sourceStats.stream().allMatch(s -> s.status == ComponentStatus.RUNNING)
                          && sinkStats.stream().allMatch(s -> s.status == ComponentStatus.RUNNING);
        String overall = anyError ? "ERROR" : allRunning ? "UP" : "STARTING";

        StringBuilder sb = new StringBuilder("{\"status\":\"").append(overall).append("\"");

        if (!sourceStats.isEmpty()) {
            sb.append(",\"sources\":[");
            for (int i = 0; i < sourceStats.size(); i++) {
                SourceStats s = sourceStats.get(i);
                if (i > 0) sb.append(",");
                sb.append("{\"name\":\"").append(s.name)
                  .append("\",\"type\":\"").append(s.type)
                  .append("\",\"status\":\"").append(s.status).append("\"}");
            }
            sb.append("]");
        }

        if (!sinkStats.isEmpty()) {
            sb.append(",\"sinks\":[");
            for (int i = 0; i < sinkStats.size(); i++) {
                SinkStats s = sinkStats.get(i);
                if (i > 0) sb.append(",");
                sb.append("{\"name\":\"").append(s.name)
                  .append("\",\"type\":\"").append(s.type)
                  .append("\",\"status\":\"").append(s.status).append("\"}");
            }
            sb.append("]");
        }

        return sb.append("}").toString();
    }

    private String buildMetricsText() {
        if (sinkStats.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        sb.append("# HELP kafka_consumer_lag Messages behind the latest offset\n");
        sb.append("# TYPE kafka_consumer_lag gauge\n");
        for (SinkStats s : sinkStats) {
            for (Map.Entry<String, Long> e : s.lagByPartition.entrySet()) {
                String[] parts     = e.getKey().split(":", 2);
                String   topic     = parts[0];
                String   partition = parts.length > 1 ? parts[1] : "0";
                sb.append("kafka_consumer_lag{sink=\"").append(s.name)
                  .append("\",topic=\"").append(topic)
                  .append("\",partition=\"").append(partition)
                  .append("\"} ").append(e.getValue()).append('\n');
            }
        }
        return sb.toString();
    }
}
