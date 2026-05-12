# kafka-ingest-pipeline

A multi-threaded Kafka ingestion pipeline that polls the
[Open-Notify ISS position API](http://api.open-notify.org/iss-now.json),
publishes records to Kafka, consumes them, and indexes them into
Elasticsearch for visualization in Kibana.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Features](#features)
3. [Prerequisites](#prerequisites)
4. [Project Structure](#project-structure)
5. [Setup](#setup)
   - [Kafka](#kafka)
   - [Elasticsearch & Kibana](#elasticsearch--kibana)
   - [Truststore for Elasticsearch TLS](#truststore-for-elasticsearch-tls)
6. [Configuration](#configuration)
   - [producer.properties](#producerproperties)
   - [consumer.properties](#consumerproperties)
   - [Message format](#message-format)
   - [Kafka security modes](#kafka-security-modes)
7. [Building](#building)
8. [Running](#running)
   - [Producer](#producer)
   - [Consumer](#consumer)
9. [Integration Test](#integration-test)
10. [Elasticsearch Index & Kibana](#elasticsearch-index--kibana)
11. [Troubleshooting](#troubleshooting)

---

## Architecture

```
+------------------------------------------------------------------+
|  Producer JVM                                                    |
|                                                                  |
|  ProducerMain                                                    |
|    |                                                             |
|    +-- [shared] KafkaProducer<String, V>  (thread-safe)         |
|    +-- [shared] DataFetcher               (Apache HttpClient)    |
|    |                                                             |
|    +-- ProducerWorker-0  --> polls ISS position API every 5 s   |
+-----------------------------+------------------------------------+
                              |  Kafka topic: open-notify-iss
                              |  (2 partitions, replication factor 1)
                              v
                    +-----------------+
                    |  Apache Kafka   |
                    |  localhost:9092 |
                    +--------+--------+
                             |
+----------------------------v-------------------------------------+
|  Consumer JVM                                                    |
|                                                                  |
|  ConsumerMain                                                    |
|    |                                                             |
|    +-- ConsumerWorker-0  (owns KafkaConsumer, 1 partition)       |
|    +-- ConsumerWorker-1  (owns KafkaConsumer, 1 partition)       |
|         |                                                        |
|         +-- ElasticsearchSink --> HTTPS, basic auth, JKS TLS    |
+-----------------------------+------------------------------------+
                              |
                    +---------v--------+
                    |  Elasticsearch   |
                    |  localhost:9200  |
                    +---------+--------+
                              |
                    +---------v--------+
                    |     Kibana       |
                    |  localhost:5601  |
                    +------------------+
```

### Threading model

| Component | Thread count | Thread safety |
|---|---|---|
| `KafkaProducer` | Shared across all producer workers | Thread-safe — one send buffer and TCP connection per broker |
| `DataFetcher` (HttpClient) | Shared across all producer workers | Thread-safe — pooled connections |
| `KafkaConsumer` | One per consumer worker | NOT thread-safe — never shared |
| `ElasticsearchSink` | One per consumer worker | Thread-safe internally but scoped to one worker |

### Delivery semantics

- **Producer** — `acks=all` + `enable.idempotence=true` gives exactly-once delivery to the broker (no duplicates on retry).
- **Consumer** — synchronous `commitSync()` after each poll batch gives at-least-once delivery to Elasticsearch. Document IDs are `<topic>-<partition>-<offset>`, so re-indexing the same record is idempotent.

---

## Features

- **Multi-threaded producer and consumer** — thread count controlled by properties file; no code changes needed.
- **Three message format modes** — JSON (default), Avro with file-based schema, or Avro with Confluent Schema Registry.
- **Generic workers** — `ProducerWorker<V>` and `ConsumerWorker<V>` handle any value type via injected converter functions; no parallel class hierarchies needed for new formats.
- **Four Kafka security modes** — PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL; switched purely by properties.
- **Elasticsearch HTTPS + basic auth + JKS truststore** — all configured via `consumer.properties`.
- **Daily rolling Elasticsearch index** — uses date-math index name `<iss-positions-{now/d{yyyy-MM-dd}}>`.
- **`recordTimestamp` enrichment** — the Unix epoch `timestamp` field from the ISS API is transparently converted to an ISO-8601 string and added to every document before indexing.
- **Graceful shutdown** — JVM shutdown hook stops workers cleanly, flushes the producer, and closes all resources.

---

## Prerequisites

| Tool | Minimum version | Notes |
|---|---|---|
| Java (JDK) | 25 | `JAVA_HOME` must be set |
| Maven | 3.9 | Used to build and run tests |
| Apache Kafka | 4.2.0 | Broker + CLI scripts; KRaft mode (no ZooKeeper) |
| Elasticsearch | 8.x / 9.x | HTTPS recommended; HTTP also supported |
| Kibana | matching ES version | Optional — for visualization |
| Confluent Schema Registry | 7.x | Optional — only for Avro + Schema Registry mode |

> **Local setup tip:** download the binary release from
> [kafka.apache.org](https://kafka.apache.org/downloads) and unzip it (e.g. to `C:\kafka`
> on Windows). Kafka 4.x uses KRaft mode — ZooKeeper is no longer required or bundled.
> Elasticsearch and Kibana can be downloaded individually from
> [elastic.co/downloads](https://www.elastic.co/downloads).

---

## Project Structure

```
kafka-ingest-pipeline/
├── config/
│   ├── producer.properties           # producer configuration
│   ├── consumer.properties           # consumer + Elasticsearch configuration
│   └── elasticsearch.truststore.jks  # local-only, excluded from git (*.jks)
├── src/
│   ├── main/java/.../kafka/
│   │   ├── common/
│   │   │   ├── AppProperties.java        # loads & validates .properties files
│   │   │   ├── AvroConverter.java        # JSON <-> Avro GenericRecord
│   │   │   ├── AvroFileDeserializer.java # Kafka Deserializer<GenericRecord> (file schema)
│   │   │   ├── AvroFileSerializer.java   # Kafka Serializer<GenericRecord>   (file schema)
│   │   │   ├── ElasticsearchSink.java    # ES client wrapper (HTTPS, auth, TLS)
│   │   │   ├── KafkaClientFactory.java   # creates typed KafkaProducer/Consumer
│   │   │   ├── MessageFormat.java        # JSON / AVRO enum
│   │   │   └── SchemaLoader.java         # loads .avsc schema from file
│   │   ├── producer/
│   │   │   ├── DataFetcher.java          # HTTP GET -> JSON string
│   │   │   ├── ProducerMain.java         # entry point, wires format + workers
│   │   │   └── ProducerWorker.java       # generic poll-convert-publish loop
│   │   └── consumer/
│   │       ├── ConsumerMain.java         # entry point, wires format + workers
│   │       └── ConsumerWorker.java       # generic poll-convert-index loop
│   └── test/java/.../kafka/
│       └── RoundTripIntegrationTest.java
├── pom.xml
└── README.md
```

---

## Setup

### Kafka

Kafka 4.x uses **KRaft mode** — ZooKeeper is no longer required or included.
The storage layer must be formatted with a cluster ID once before the broker can start.
The cluster metadata is persisted under the directory specified by `log.dir` in
`config/server.properties` (default `C:\kafka\logs\kraft-combined-logs` on Windows).

**1. Format storage (first time only)**

```bash
# Linux/macOS
bin/kafka-storage.sh random-uuid          # generates a cluster UUID, copy the output
bin/kafka-storage.sh format -t <uuid> -c config/server.properties --standalone

# Windows (from C:\kafka)
bin\windows\kafka-storage.bat random-uuid
bin\windows\kafka-storage.bat format -t <uuid> -c config\server.properties --standalone
```

This step only needs to be done once. The `meta.properties` file written into the
log directory will be reused on all subsequent starts.

**2. Start the broker**

```bash
# Linux/macOS
bin/kafka-server-start.sh config/server.properties

# Windows (from C:\kafka)
bin\windows\kafka-server-start.bat config\server.properties
```

**3. Create the topic**

```bash
# Linux/macOS
bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic open-notify-iss \
  --partitions 2 \
  --replication-factor 1

# Windows (from C:\kafka)
bin\windows\kafka-topics.bat --create ^
  --bootstrap-server localhost:9092 ^
  --topic open-notify-iss ^
  --partitions 2 ^
  --replication-factor 1
```

**4. Verify**

```bash
# Linux/macOS
bin/kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic open-notify-iss

# Windows
bin\windows\kafka-topics.bat --describe --bootstrap-server localhost:9092 --topic open-notify-iss
```

---

### Elasticsearch & Kibana

**Start Elasticsearch**

```bash
# Linux/macOS (from Elasticsearch install directory)
bin/elasticsearch

# Windows (from C:\elasticsearch)
bin\elasticsearch.bat
```

Elasticsearch listens on `https://localhost:9200` by default with security enabled.

**Start Kibana**

```bash
# Linux/macOS (from Kibana install directory)
bin/kibana

# Windows (from C:\kibana)
bin\kibana.bat
```

Kibana listens on `http://localhost:5601`.

**Reset the `elastic` user password** (first-time setup or if you need to change it):

```bash
# From the Elasticsearch install directory
bin/elasticsearch-reset-password -u elastic        # Linux/macOS
bin\elasticsearch-reset-password.bat -u elastic    # Windows
```

Update `consumer.properties` with the password:

```properties
elasticsearch.username=elastic
elasticsearch.password=your-password-here
```

**Set index replicas to 0** (single-node setup only — prevents yellow index status):

```bash
# Linux/macOS
curl -X PUT "https://localhost:9200/open-notify-iss/_settings" \
  -u elastic:<password> \
  -H "Content-Type: application/json" \
  -d '{"index":{"number_of_replicas":0}}'

# Windows (PowerShell — escape inner double quotes)
curl -X PUT "https://localhost:9200/open-notify-iss/_settings" `
  -u elastic:<password> `
  -H "Content-Type: application/json" `
  -d "{\"index\":{\"number_of_replicas\":0}}"
```

---

### Truststore for Elasticsearch TLS

The Elasticsearch HTTPS certificate needs to be trusted by the Java consumer.
Export the Elasticsearch CA certificate and import it into a JKS truststore:

```bash
# The CA cert is typically at: <ES install dir>/config/certs/http_ca.crt

keytool -importcert \
  -alias elasticsearch-ca \
  -file /path/to/http_ca.crt \
  -keystore config/elasticsearch.truststore.jks \
  -storepass changeit \
  -noprompt
```

The `*.jks` file is excluded from version control via `.gitignore`.
Each developer must generate their own truststore from their local Elasticsearch instance.

Verify or update these properties in `consumer.properties`:

```properties
elasticsearch.scheme=https
elasticsearch.ssl.truststore.location=config/elasticsearch.truststore.jks
elasticsearch.ssl.truststore.password=changeit
```

---

## Configuration

### producer.properties

| Property | Default | Description |
|---|---|---|
| `bootstrap.servers` | *(required)* | Kafka broker address(es) |
| `topic.name` | *(required)* | Topic to produce to |
| `num.producer.threads` | *(required)* | Number of producer worker threads |
| `data.source.iss.position` | *(required)* | ISS position API URL |
| `polling.interval.ms` | `5000` | How often each worker polls the API (ms) |
| `message.format` | `json` | `json` or `avro` |
| `message.schema.file` | — | Path to `.avsc` schema file (Avro file mode) |
| `message.schema.registry.url` | — | Schema Registry URL (Avro registry mode) |
| `acks` | `1` | Producer acknowledgement (`all` required for idempotence) |
| `enable.idempotence` | — | `true` prevents duplicate messages on retry |
| `compression.type` | `none` | `none`, `snappy`, `lz4`, `gzip`, `zstd` |

### consumer.properties

| Property | Default | Description |
|---|---|---|
| `bootstrap.servers` | *(required)* | Kafka broker address(es) |
| `group.id` | *(required)* | Consumer group ID |
| `topic.name` | *(required)* | Topic to consume from |
| `num.consumer.threads` | *(required)* | Worker threads (must be <= partition count) |
| `message.format` | `json` | `json` or `avro` — must match the producer |
| `message.schema.file` | — | Path to `.avsc` schema file (Avro file mode) |
| `message.schema.registry.url` | — | Schema Registry URL (Avro registry mode) |
| `auto.offset.reset` | `earliest` | `earliest` or `latest` |
| `elasticsearch.host` | `localhost` | Elasticsearch hostname |
| `elasticsearch.port` | `9200` | Elasticsearch port |
| `elasticsearch.scheme` | `http` | `http` or `https` |
| `elasticsearch.index` | *(required)* | Index name; date-math syntax supported |
| `elasticsearch.username` | — | Basic auth username |
| `elasticsearch.password` | — | Basic auth password |
| `elasticsearch.ssl.truststore.location` | — | JKS truststore path (HTTPS only) |
| `elasticsearch.ssl.truststore.password` | — | Truststore password |

### Message format

Controlled by `message.format` in both properties files.

| Value | Serialization | Extra properties required |
|---|---|---|
| `json` (default) | Raw JSON string, no schema | None |
| `avro` + `message.schema.file` | Avro binary, file-based schema | `message.schema.file=/path/to/schema.avsc` |
| `avro` + `message.schema.registry.url` | Avro binary, Confluent Schema Registry | `message.schema.registry.url=http://localhost:8081` |

The producer and consumer must use **the same format and the same schema**.

### Kafka security modes

Controlled by `security.protocol` (and supporting properties) in each properties file.
Uncomment exactly one block in `producer.properties` and `consumer.properties`.

| Mode | `security.protocol` | Encryption | Authentication | Extra properties needed |
|---|---|---|---|---|
| 1 — Local dev (default) | *(absent)* | None | None | Nothing — works out of the box |
| 2 — TLS only | `SSL` | TLS | None (one-way cert validation) | `ssl.truststore.*`; add `ssl.keystore.*` for mTLS |
| 3 — SASL, no TLS | `SASL_PLAINTEXT` | None | Username + password | `sasl.mechanism` + `sasl.jaas.config` |
| 4 — SASL over TLS | `SASL_SSL` | TLS | Username + password | `ssl.truststore.*` + `sasl.mechanism` + `sasl.jaas.config` |

**PLAIN mechanism example** (credentials stored on the broker):

```properties
security.protocol=SASL_SSL
ssl.truststore.location=/path/to/client.truststore.jks
ssl.truststore.password=changeit
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="aditya" \
  password="aditya-secret";
```

**SCRAM-SHA-256 example** (broker must have SCRAM credentials stored):

```properties
security.protocol=SASL_SSL
ssl.truststore.location=/path/to/client.truststore.jks
ssl.truststore.password=changeit
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="aditya" \
  password="aditya-secret";
```

For mTLS (broker also validates the client certificate) add the keystore properties
to any SSL or SASL_SSL block:

```properties
ssl.keystore.location=/path/to/client.keystore.jks
ssl.keystore.password=changeit
ssl.key.password=changeit
```

---

## Building

```bash
mvn clean package
```

Produces `target/kafka-ingest-pipeline-1.0.0-fat.jar` — a single uber-jar with all
dependencies bundled via the Maven Shade Plugin. The jar has no `Main-Class` manifest
entry; the entry point is chosen at runtime via `-cp`.

---

## Running

Both producer and consumer accept a single argument: the path to their properties file.

### Producer

```bash
# Linux/macOS
java -cp target/kafka-ingest-pipeline-1.0.0-fat.jar \
  io.github.adityassharma.kafka.producer.ProducerMain \
  config/producer.properties

# Windows
java -cp target\kafka-ingest-pipeline-1.0.0-fat.jar io.github.adityassharma.kafka.producer.ProducerMain config\producer.properties
```

Each worker thread polls the ISS position API every `polling.interval.ms` milliseconds
and publishes the raw JSON to the Kafka topic. The `KafkaProducer` is shared across all
workers (thread-safe; one TCP connection per broker).

Send `SIGINT` (Ctrl-C) to trigger graceful shutdown — the producer flushes in-flight
messages, closes cleanly, then exits.

### Consumer

```bash
# Linux/macOS
java -cp target/kafka-ingest-pipeline-1.0.0-fat.jar \
  io.github.adityassharma.kafka.consumer.ConsumerMain \
  config/consumer.properties

# Windows
java -cp target\kafka-ingest-pipeline-1.0.0-fat.jar io.github.adityassharma.kafka.consumer.ConsumerMain config\consumer.properties
```

Each worker thread owns its own `KafkaConsumer` (not thread-safe; never shared),
subscribes to the topic via the group protocol, and indexes every record into
Elasticsearch. Offsets are committed synchronously after every poll batch
(at-least-once delivery).

Before indexing, `ElasticsearchSink` adds a `recordTimestamp` field (ISO-8601)
derived from the `timestamp` Unix epoch field in the ISS JSON payload.

Document IDs are `<topic>-<partition>-<offset>`, making re-indexing idempotent.

Send `SIGINT` (Ctrl-C) to shut down — workers finish their current poll batch,
commit offsets, and close.

---

## Integration Test

`RoundTripIntegrationTest` produces 10 synthetic ISS-like messages to Kafka and verifies
that all 10 are received back by a consumer, checking both count and content.

**Prerequisites:**
- Kafka running on `localhost:9092`
- Topic `open-notify-iss` exists with at least 1 partition
- Elasticsearch is **not** required

```bash
mvn test -Pintegration
```

Unit tests are skipped by default (`skipTests=true` in `pom.xml`).
The `integration` Maven profile re-enables Surefire.

The test uses `assign()` + `seekToEnd()` + `position()` (instead of `subscribe()`)
to avoid the asynchronous group-rebalance race condition: the consumer is deterministically
positioned at the end of each partition before the producer sends anything.

---

## Elasticsearch Index & Kibana

### Index naming

The default index name in `consumer.properties` uses Elasticsearch date-math syntax:

```
<iss-positions-{now/d{yyyy-MM-dd}}>
```

This resolves to a daily index such as `iss-positions-2026-05-12`. A new index is
created automatically at midnight; no manual index management is required.

To use a static index name instead:

```properties
elasticsearch.index=iss-positions
```

### Creating a Kibana data view

1. Open Kibana → **Management → Stack Management → Data Views**
2. Click **Create data view**
3. Set the index pattern to `iss-positions-*`
4. Set the time field to `recordTimestamp`
5. Save, then open **Discover** to browse incoming records

### Key document fields

| Field | Type | Description |
|---|---|---|
| `timestamp` | `long` | Unix epoch seconds from the ISS API |
| `recordTimestamp` | `date` | ISO-8601 version of `timestamp`, added by the consumer |
| `iss_position.latitude` | `text` | ISS latitude |
| `iss_position.longitude` | `text` | ISS longitude |
| `message` | `text` | API status (`"success"`) |

---

## Troubleshooting

**Consumer starts but receives no messages**
- Confirm the producer is running and `topic.name` matches in both properties files.
- Check `auto.offset.reset=earliest` in `consumer.properties` if you want to replay
  messages already in the topic.
- Verify `num.consumer.threads` is not greater than the number of partitions — excess
  threads receive no partition assignment and sit idle.

**`ConfigException: Must set acks to all in enable.idempotence=true`**
- Set `acks=all` in `producer.properties`. Idempotent producers require all in-sync
  replicas to acknowledge.

**`Connection is closed` or SSL handshake errors (Elasticsearch)**
- Confirm `elasticsearch.scheme=https` in `consumer.properties`.
- Confirm the JKS truststore contains the Elasticsearch CA certificate.
- Verify the truststore path and password.

**`NoClassDefFoundError` when running from IntelliJ**
- Run using the fat jar instead of the IDE classpath:
  ```bash
  java -cp target/kafka-ingest-pipeline-1.0.0-fat.jar \
    io.github.adityassharma.kafka.consumer.ConsumerMain \
    config/consumer.properties
  ```

**Yellow Elasticsearch index status**
- Single-node clusters cannot satisfy the default replica count of 1.
  Set replicas to 0 (see [Elasticsearch & Kibana setup](#elasticsearch--kibana)).

**Integration test receives 0 messages**
- Confirm the topic exists: `kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic open-notify-iss`
- The test uses `assign()` + `seekToEnd()` which resolves positions before the producer
  sends, so a rebalance race is not the cause — the topic simply must exist with at
  least 1 partition.
