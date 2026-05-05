package io.github.adityassharma.kafka.common;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;

import java.io.Closeable;
import java.io.IOException;
import java.io.StringReader;

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
     * </ul>
     */
    public ElasticsearchSink(AppProperties appProps) {
        String host   = appProps.get("elasticsearch.host",   "localhost");
        int    port   = appProps.getInt("elasticsearch.port",  9200);
        String scheme = appProps.get("elasticsearch.scheme", "http");
        this.indexName = appProps.getRequired("elasticsearch.index");

        LOG.info("Connecting to Elasticsearch at {}://{}:{} index={}",
            scheme, host, port, indexName);

        // Low-level REST client (manages HTTP connections)
        this.restClient = RestClient.builder(new HttpHost(host, port, scheme)).build();

        // High-level Elasticsearch Java client
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
            IndexResponse response = esClient.index(i -> i
                .index(indexName)
                .id(docId)
                .withJson(new StringReader(jsonDoc))
            );
            LOG.debug("Indexed doc id={} result={} index={}",
                response.id(), response.result().jsonValue(), indexName);
        } catch (Exception e) {
            LOG.error("Failed to index document id={}: {}", docId, e.getMessage(), e);
            // Do not re-throw — allow the consumer thread to continue processing
            // subsequent messages. Consider a dead-letter queue for production.
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
}
