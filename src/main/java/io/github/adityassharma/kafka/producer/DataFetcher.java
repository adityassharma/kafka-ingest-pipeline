package io.github.adityassharma.kafka.producer;

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
 * Fetches JSON payloads from public REST APIs with no authentication.
 *
 * <p>Two data sources are used:
 * <ol>
 *   <li><b>ISS Current Position</b> — {@code http://api.open-notify.org/iss-now.json}
 *       Returns lat/lon of the International Space Station, updated every ~5 s.
 *       Example response:
 *       <pre>
 *       {"message": "success", "timestamp": 1712345678,
 *        "iss_position": {"latitude": "51.5193", "longitude": "-0.1277"}}
 *       </pre>
 *   </li>
 *   <li><b>People in Space</b> — {@code http://api.open-notify.org/astros.json}
 *       Returns the list of people currently aboard the ISS. Changes rarely.
 *       Example response:
 *       <pre>
 *       {"message": "success", "number": 7,
 *        "people": [{"name": "Oleg Kononenko", "craft": "ISS"}, ...]}
 *       </pre>
 *   </li>
 * </ol>
 *
 * <p>Thread-safety: the underlying Apache HttpClient is thread-safe; a single
 * shared instance is used across both producer threads.
 */
public class DataFetcher implements Closeable {

    private static final Logger LOG = LogManager.getLogger(DataFetcher.class);

    private final CloseableHttpClient httpClient;

    public DataFetcher() {
        // Default connection pool — handles both threads without contention
        this.httpClient = HttpClients.createDefault();
    }

    /**
     * Perform a blocking HTTP GET and return the response body as a UTF-8 string.
     *
     * @param url the URL to fetch
     * @return JSON response body, or {@code null} on error (caller decides whether to skip)
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
                String body = EntityUtils.toString(response.getEntity(),
                    StandardCharsets.UTF_8);
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
