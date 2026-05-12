package io.github.adityassharma.kafka.common;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.security.KeyStore;
import java.time.Instant;

/**
 * Thread-safe wrapper around the Elasticsearch Java client.
 *
 * <p>One {@code ElasticsearchSink} instance is created per consumer worker thread
 * and closed when the thread exits.  The underlying HTTP client uses a connection
 * pool, so creating one per thread is fine at our scale (2–4 threads).
 *
 * <p>Implements {@link Closeable} for use in try-with-resources.
 */
public class ElasticsearchSink implements Closeable {

    private static final Logger LOG = LogManager.getLogger(ElasticsearchSink.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ElasticsearchClient esClient;
    private final RestClient restClient;
    private final String indexName;

    /**
     * Construct a sink connected to the Elasticsearch node described in {@code appProps}.
     *
     * <p>Properties used:
     * <ul>
     *   <li>{@code elasticsearch.host} (default: localhost)</li>
     *   <li>{@code elasticsearch.port} (default: 9200)</li>
     *   <li>{@code elasticsearch.scheme} (default: http)</li>
     *   <li>{@code elasticsearch.index} (required)</li>
     *   <li>{@code elasticsearch.username} (optional)</li>
     *   <li>{@code elasticsearch.password} (optional)</li>
     *   <li>{@code elasticsearch.ssl.truststore.location} (optional, JKS)</li>
     *   <li>{@code elasticsearch.ssl.truststore.password} (optional)</li>
     * </ul>
     */
    public ElasticsearchSink(AppProperties appProps) {
        String host   = appProps.get("elasticsearch.host",   "localhost");
        int    port   = appProps.getInt("elasticsearch.port",  9200);
        String scheme = appProps.get("elasticsearch.scheme", "http");
        this.indexName = appProps.getRequired("elasticsearch.index");

        LOG.info("Connecting to Elasticsearch at {}://{}:{} index={}",
            scheme, host, port, indexName);

        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, scheme));

        String truststorePath     = appProps.get("elasticsearch.ssl.truststore.location", null);
        String truststorePassword = appProps.get("elasticsearch.ssl.truststore.password", null);
        String username           = appProps.get("elasticsearch.username", null);
        String password           = appProps.get("elasticsearch.password", null);

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

        this.restClient = builder.build();

        ElasticsearchTransport transport =
            new RestClientTransport(restClient, new JacksonJsonpMapper());
        this.esClient = new ElasticsearchClient(transport);
    }

    /**
     * Index a JSON document string into Elasticsearch.
     *
     * @param docId  document ID (use Kafka offset or a field from the payload)
     * @param jsonDoc the raw JSON string to index
     */
    public void index(String docId, String jsonDoc) {
        try {
            String enriched = injectRecordTimestamp(jsonDoc);
            IndexResponse response = esClient.index(i -> i
                .index(indexName)
                .id(docId)
                .withJson(new StringReader(enriched))
            );
            LOG.debug("Indexed doc id={} result={} index={}",
                response.id(), response.result().jsonValue(), indexName);
        } catch (Exception e) {
            LOG.error("Failed to index document id={}: {}", docId, e.getMessage(), e);
            // Do not re-throw — allow the consumer thread to continue processing
            // subsequent messages. Consider a dead-letter queue for production.
        }
    }

    // Adds recordTimestamp (ISO-8601) derived from the epoch-second timestamp field.
    // If timestamp is absent or not a number the document is returned unchanged.
    private static String injectRecordTimestamp(String jsonDoc) {
        try {
            ObjectNode node = (ObjectNode) MAPPER.readTree(jsonDoc);
            if (node.has("timestamp") && node.get("timestamp").isNumber()) {
                long epochSeconds = node.get("timestamp").asLong();
                node.put("recordTimestamp", Instant.ofEpochSecond(epochSeconds).toString());
            } else {
                LOG.warn("timestamp field absent or non-numeric — recordTimestamp not added");
            }
            return MAPPER.writeValueAsString(node);
        } catch (Exception e) {
            LOG.warn("Failed to inject recordTimestamp, forwarding original document: {}", e.getMessage());
            return jsonDoc;
        }
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing Elasticsearch sink for index={}", indexName);
        try {
            restClient.close();
        } catch (IOException e) {
            LOG.warn("Error closing Elasticsearch REST client: {}", e.getMessage());
            throw e;
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
