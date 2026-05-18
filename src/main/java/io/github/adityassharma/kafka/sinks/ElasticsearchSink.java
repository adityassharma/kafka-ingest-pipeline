package io.github.adityassharma.kafka.sinks;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.adityassharma.kafka.spi.Record;
import io.github.adityassharma.kafka.spi.Sink;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.security.KeyStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Sink that writes records to Elasticsearch using the Bulk API.
 *
 * <p>An entire Kafka poll batch is sent in a single bulk request.
 * Per-item failure tracking is enabled only when {@code dlq.topic} is configured,
 * avoiding the cost of iterating the bulk response on the happy path.
 *
 * <p>Thread-safe: one instance is shared across all SinkRunner worker threads.
 *
 * <p>Config keys (after prefix stripping):
 * <ul>
 *   <li>{@code elasticsearch.host} (default: localhost)</li>
 *   <li>{@code elasticsearch.port} (default: 9200)</li>
 *   <li>{@code elasticsearch.scheme} (default: http)</li>
 *   <li>{@code elasticsearch.index} (required)</li>
 *   <li>{@code elasticsearch.username} (optional)</li>
 *   <li>{@code elasticsearch.password} (optional)</li>
 *   <li>{@code elasticsearch.ssl.truststore.location} (optional)</li>
 *   <li>{@code elasticsearch.ssl.truststore.password} (optional)</li>
 *   <li>{@code dlq.topic} (optional — enables per-item failure tracking)</li>
 * </ul>
 */
public class ElasticsearchSink implements Sink {

    private static final Logger LOG = LogManager.getLogger(ElasticsearchSink.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ElasticsearchClient esClient;
    private RestClient restClient;
    private String indexName;
    private boolean perItemTracking;

    @Override
    public String type() {
        return "elasticsearch";
    }

    @Override
    public void configure(Properties props) {
        String host   = props.getProperty("elasticsearch.host",   "localhost");
        int    port   = Integer.parseInt(props.getProperty("elasticsearch.port",   "9200"));
        String scheme = props.getProperty("elasticsearch.scheme", "http");
        indexName     = props.getProperty("elasticsearch.index");
        if (indexName == null || indexName.isBlank()) {
            throw new IllegalArgumentException("elasticsearch.index is required");
        }

        // Per-item failure tracking only pays off when there is a DLQ to route to.
        perItemTracking = props.getProperty("dlq.topic") != null;

        LOG.info("ElasticsearchSink connecting to {}://{}:{} index={} perItemTracking={}",
            scheme, host, port, indexName, perItemTracking);

        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, scheme));

        String truststorePath     = props.getProperty("elasticsearch.ssl.truststore.location");
        String truststorePassword = props.getProperty("elasticsearch.ssl.truststore.password");
        String username           = props.getProperty("elasticsearch.username");
        String password           = props.getProperty("elasticsearch.password");

        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            if (truststorePath != null) {
                httpClientBuilder.setSSLContext(buildSslContext(truststorePath, truststorePassword));
            }
            if (username != null) {
                BasicCredentialsProvider cp = new BasicCredentialsProvider();
                cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
                httpClientBuilder.setDefaultCredentialsProvider(cp);
            }
            return httpClientBuilder;
        });

        restClient = builder.build();
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        esClient = new ElasticsearchClient(transport);
    }

    /**
     * Send the entire batch to Elasticsearch in a single bulk request.
     *
     * <p>When DLQ is not configured ({@code perItemTracking=false}), only the
     * top-level {@code errors} flag is checked — O(1).  When DLQ is configured,
     * each item's status is checked and failed items are returned for DLQ routing.
     *
     * @return failed records (empty when all succeeded or when perItemTracking is off)
     */
    @Override
    public List<Record> writeBatch(List<Record> records) throws Exception {
        if (records.isEmpty()) return Collections.emptyList();

        BulkRequest.Builder bulkBuilder = new BulkRequest.Builder();
        for (Record r : records) {
            String docId   = r.topic() + "-" + r.partition() + "-" + r.offset();
            String enriched = injectRecordTimestamp(r.value(), r.timestamp());
            final String enrichedFinal = enriched;
            bulkBuilder.operations(op -> op
                .index(idx -> idx
                    .index(indexName)
                    .id(docId)
                    .withJson(new StringReader(enrichedFinal))
                )
            );
        }

        BulkResponse response = esClient.bulk(bulkBuilder.build());

        if (!perItemTracking) {
            if (response.errors()) {
                LOG.error("Bulk request had errors (perItemTracking disabled) — skipping failed items");
            } else {
                LOG.debug("Bulk indexed {} documents to {}", records.size(), indexName);
            }
            return Collections.emptyList();
        }

        // Per-item tracking: collect failures for DLQ routing.
        List<Record> failures = new ArrayList<>();
        List<BulkResponseItem> items = response.items();
        for (int i = 0; i < items.size(); i++) {
            BulkResponseItem item = items.get(i);
            if (item.error() != null) {
                LOG.warn("Bulk item failed id={} error={}", item.id(), item.error().reason());
                failures.add(records.get(i));
            }
        }
        if (!failures.isEmpty()) {
            LOG.error("Bulk request: {}/{} items failed", failures.size(), records.size());
        } else {
            LOG.debug("Bulk indexed {} documents to {}", records.size(), indexName);
        }
        return failures;
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing ElasticsearchSink for index={}", indexName);
        if (restClient != null) restClient.close();
    }

    private static String injectRecordTimestamp(String jsonDoc, Instant timestamp) {
        try {
            ObjectNode node = (ObjectNode) MAPPER.readTree(jsonDoc);
            if (node.has("timestamp") && node.get("timestamp").isNumber()) {
                long epochSeconds = node.get("timestamp").asLong();
                node.put("recordTimestamp", Instant.ofEpochSecond(epochSeconds).toString());
            } else if (timestamp != null) {
                node.put("recordTimestamp", timestamp.toString());
            }
            return MAPPER.writeValueAsString(node);
        } catch (Exception e) {
            LOG.warn("Failed to inject recordTimestamp, forwarding original: {}", e.getMessage());
            return jsonDoc;
        }
    }

    private static SSLContext buildSslContext(String truststorePath, String truststorePassword) {
        try {
            KeyStore truststore = KeyStore.getInstance("JKS");
            char[] password = truststorePassword != null ? truststorePassword.toCharArray() : null;
            try (FileInputStream fis = new FileInputStream(truststorePath)) {
                truststore.load(fis, password);
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(truststore);
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);
            return sslContext;
        } catch (Exception e) {
            throw new RuntimeException("Failed to build SSLContext from truststore: " + truststorePath, e);
        }
    }
}
