package io.github.adityassharma.kafka.sources.util;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Thin HTTP GET wrapper used by polling-based sources.
 * Thread-safe; a single instance can be shared across threads.
 */
public class DataFetcher implements Closeable {

    private static final Logger LOG = LogManager.getLogger(DataFetcher.class);

    private final CloseableHttpClient httpClient;

    public DataFetcher() {
        this.httpClient = HttpClients.createDefault();
    }

    /**
     * Perform a blocking HTTP GET and return the response body as a UTF-8 string.
     *
     * @param url the URL to fetch
     * @return JSON response body, or {@code null} on any error
     */
    public String fetch(String url) {
        HttpGet request = new HttpGet(url);
        try {
            return httpClient.execute(request, response -> {
                int statusCode = response.getCode();
                if (statusCode != 200) {
                    LOG.warn("HTTP {} received from {}", statusCode, url);
                    return null;
                }
                String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                LOG.debug("Fetched {} bytes from {}", body.length(), url);
                return body;
            });
        } catch (IOException e) {
            LOG.error("Failed to fetch {}: {}", url, e.getMessage());
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }
}
