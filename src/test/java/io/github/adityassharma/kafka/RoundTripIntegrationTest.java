package io.github.adityassharma.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Integration test: produce synthetic ISS-like JSON messages to Kafka, then
 * consume them from the same topic and verify round-trip correctness.
 *
 * <h2>Prerequisites</h2>
 * <ul>
 *   <li>Kafka must be running on {@code localhost:9092}</li>
 *   <li>Topic {@code open-notify-iss} must exist with ≥ 1 partition</li>
 *   <li>Elasticsearch is NOT required for this test</li>
 * </ul>
 *
 * <h2>Running</h2>
 * <pre>
 *   mvn test -Pintegration
 * </pre>
 *
 * <p>By default, Surefire skips tests (skipTests=true). Run with the
 * {@code integration} profile to execute them.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RoundTripIntegrationTest {

    // ---------------------------------------------------------------------------
    // Configuration — mirrors consumer.properties / producer.properties so
    // the test can run without those files present (useful in CI pipelines).
    // ---------------------------------------------------------------------------
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC             = "open-notify-iss";
    private static final int    NUM_MESSAGES      = 10;
    private static final int    POLL_TIMEOUT_SEC  = 10;

    /** Unique group ID per test run so we always start from the latest offset. */
    private static final String GROUP_ID = "test-group-" + UUID.randomUUID();

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;

    // -----------------------------------------------------------------------
    // Setup / Teardown
    // -----------------------------------------------------------------------

    @BeforeAll
    void setUp() {
        producer = buildProducer();
        consumer = buildConsumer();
    }

    @AfterAll
    void tearDown() {
        if (consumer != null) consumer.close();
        if (producer != null) { producer.flush(); producer.close(); }
    }

    // -----------------------------------------------------------------------
    // Test
    // -----------------------------------------------------------------------

    /**
     * Produce {@value NUM_MESSAGES} synthetic messages, then consume and assert
     * that all messages are received with correct content.
     */
    @Test
    void testRoundTrip() throws Exception {
        System.out.println("=== Round-trip test: producing " + NUM_MESSAGES + " messages ===");

        // ---- Produce ----
        List<String> sentValues = new ArrayList<>();
        for (int i = 0; i < NUM_MESSAGES; i++) {
            String json = buildSyntheticIssJson(i);
            sentValues.add(json);

            ProducerRecord<String, String> record =
                new ProducerRecord<>(TOPIC, "test-key-" + i, json);

            RecordMetadata meta = producer.send(record).get();  // blocking send
            System.out.printf("  Sent #%d → topic=%s partition=%d offset=%d%n",
                i, meta.topic(), meta.partition(), meta.offset());
        }
        producer.flush();

        // ---- Consume ----
        List<String> receivedValues = new ArrayList<>();
        long deadline = System.currentTimeMillis() + (POLL_TIMEOUT_SEC * 1000L);

        System.out.println("  Consumer assignment: " + consumer.assignment());
        System.out.println("  Consumer positions:  " +
            consumer.assignment().stream()
                .collect(Collectors.toMap(tp -> tp, consumer::position)));

        while (receivedValues.size() < NUM_MESSAGES
               && System.currentTimeMillis() < deadline) {

            try {
                ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofSeconds(2));

                System.out.printf("  poll() returned %d record(s)%n", records.count());
                for (ConsumerRecord<String, String> r : records) {
                    receivedValues.add(r.value());
                    System.out.printf("  Received: topic=%s partition=%d offset=%d value=%s%n",
                        r.topic(), r.partition(), r.offset(), r.value());
                }
            } catch (Exception e) {
                System.err.println("  poll() threw exception: " + e);
                throw e;
            }
        }

        // ---- Assert ----
        assertEquals(NUM_MESSAGES, receivedValues.size(),
            "Expected " + NUM_MESSAGES + " messages but received " + receivedValues.size());

        for (String sent : sentValues) {
            assertTrue(receivedValues.contains(sent),
                "Sent value not found in received messages: " + sent);
        }

        System.out.println("=== Round-trip test PASSED ===");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private KafkaProducer<String, String> buildProducer() {
        java.util.Properties props = new java.util.Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "1");
        props.put("retries", "3");
        props.put("enable.idempotence", "false"); // simpler for testing
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, String> buildConsumer() {
        java.util.Properties props = new java.util.Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", GROUP_ID);
        props.put("key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        props.put("enable.auto.commit", "true");
        props.put("max.poll.records", "100");

        KafkaConsumer<String, String> c = new KafkaConsumer<>(props);

        // Manual assign + seekToEnd avoids the async group-rebalance race:
        // subscribe() hands partition assignment to the coordinator asynchronously,
        // so a timed dummy poll may return before partitions are assigned and the
        // consumer would miss messages sent immediately after setUp() returns.
        // assign() is synchronous; seekToEnd() positions each partition at the
        // current end; position() forces the lazy seek to resolve before the
        // producer sends anything.
        List<TopicPartition> partitions = c.partitionsFor(TOPIC).stream()
            .map(p -> new TopicPartition(TOPIC, p.partition()))
            .collect(Collectors.toList());
        System.out.println("  Assigning partitions: " + partitions);
        c.assign(partitions);
        c.seekToEnd(partitions);
        partitions.forEach(tp ->
            System.out.printf("  Seeked %s to end offset %d%n", tp, c.position(tp)));
        return c;
    }

    /**
     * Build a realistic ISS position JSON payload for message {@code index}.
     * Mimics the Open-Notify API response shape.
     */
    private static String buildSyntheticIssJson(int index) {
        // Vary lat/lon slightly for each message so values are distinguishable
        double lat = -90.0 + (index * 18.0);   // spread across -90..90
        double lon = -180.0 + (index * 36.0);  // spread across -180..180
        long   ts  = System.currentTimeMillis() / 1000L + index;

        return String.format(
            "{\"message\":\"success\",\"timestamp\":%d," +
            "\"iss_position\":{\"latitude\":\"%.4f\",\"longitude\":\"%.4f\"}," +
            "\"test_index\":%d}",
            ts, lat, lon, index);
    }
}
