# kafka-ingest-pipeline

> **A note on production use**
>
> In a real production environment you would almost certainly reach for
> [Kafka Connect](https://kafka.apache.org/documentation/#connect) instead of building
> your own pipeline framework. Kafka Connect ships with hundreds of battle-tested
> connectors (Elasticsearch, S3, JDBC, MongoDB, …), a distributed worker cluster for
> horizontal scaling, a REST API for deploying and reconfiguring connectors at runtime
> without restarts, and deep integration with Confluent Schema Registry and Single Message
> Transforms (SMTs).
>
> **This project is primarily a learning exercise** — it is built on exactly the same
> conceptual foundation as Kafka Connect (pluggable sources and sinks, SPI discovery,
> topic-based routing, consumer-group fan-out, DLQ, at-least-once delivery with idempotent
> writes) so working through it gives you a solid mental model of what Kafka Connect is
> doing under the hood.
>
> That said, this codebase does have genuine utility where Kafka Connect would be
> overkill or operationally inconvenient:
> - **Zero cluster overhead** — a single fat JAR and a properties file is all you need.
>   Kafka Connect in distributed mode requires running and monitoring a separate fleet of
>   worker processes with their own REST API and coordinator.
> - **Code-first configuration** — sources and sinks are plain Java classes; there are no
>   JSON connector configs, no converter/transform chains, and no plugin-path management.
>   Changing behaviour means changing code, not wrestling with a configuration DSL.
> - **Transparent internals** — the producer, consumer, serialiser, offset commit, and
>   DLQ logic are all directly visible and debuggable. Kafka Connect buries these behind
>   several abstraction layers which makes tracing a bug significantly harder.
> - **Embeddable** — the pipeline can be started inside a larger application with a single
>   method call; no external process or sidecar required.
> - **Custom business logic** — complex pre/post processing logic lives in a regular Java
>   class rather than a chain of SMTs.
> - **Avro without Schema Registry** — file-based Avro schema mode works with no
>   additional infrastructure.
>
> In short: use this to learn, to prototype, or to run a lightweight pipeline where
> standing up a Kafka Connect cluster is more trouble than it is worth. Graduate to Kafka
> Connect when you need its connector ecosystem, distributed scaling, or live
> reconfiguration.

---

A production-grade, pluggable Kafka ingestion pipeline modelled on the Kafka Connect
architecture. Sources and sinks are discovered at runtime via Java's
**Service Provider Interface (SPI)**, allowing new data sources and destinations to be
added without touching core framework code.

The default configuration polls the
[Open-Notify ISS position API](http://api.open-notify.org/iss-now.json),
publishes records to Kafka, and writes them to Elasticsearch for visualization in Kibana —
while simultaneously fanning out to a `LoggingSink` to demonstrate multiple sinks on the
same topic.

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
   - [pipeline.properties](#pipelineproperties)
   - [Routing: sources → topics → sinks](#routing-sources--topics--sinks)
   - [Fan-out: one source to multiple sinks](#fan-out-one-source-to-multiple-sinks)
   - [Dead Letter Queue (DLQ)](#dead-letter-queue-dlq)
   - [Single Message Transforms (SMTs)](#single-message-transforms-smts)
   - [Message format](#message-format)
   - [Kafka security modes](#kafka-security-modes)
   - [Health & Metrics](#health--metrics)
7. [Adding a new Source, Sink, or Transform](#adding-a-new-source-sink-or-transform)
8. [Building](#building)
9. [Running](#running)
10. [Integration Test](#integration-test)
11. [Elasticsearch Index & Kibana](#elasticsearch-index--kibana)
12. [Troubleshooting](#troubleshooting)

---

## Architecture

```
config/pipeline.properties
         |
         v
  PipelineMain  (discovers Source/Sink impls via ServiceLoader)
         |
         +---------- SourceRunner (per source instance)
         |                |
         |                +-- IssApiSource     --> polls ISS API every 5 s
         |                +-- HttpListenerSource --> accepts HTTP POST (FluentBit etc.)
         |                |
         |                +-- KafkaProducer<String, V>  [shared, thread-safe]
         |
         +---------- SinkRunner (per sink instance)
                          |
                          +-- ConsumerWorker threads, each with own KafkaConsumer
                          |        |
                          |        v
                          |   Sink.writeBatch(List<Record>)  [one bulk request]
                          |        |
                          |        +-- ElasticsearchSink  (Bulk API, HTTPS, auth, TLS)
                          |        +-- LoggingSink         (Log4j2)
                          |
                          +-- DLQ KafkaProducer (optional, one per SinkRunner)
```

```
  Sources                Kafka Broker           Sinks
  ───────                ────────────           ─────
  IssApiSource  ──┐                    ┌──── ElasticsearchSink  (group: iss-es-consumer)
                  ├─► open-notify-iss ─┤
  HttpListener  ──┘                    └──── LoggingSink         (group: iss-log-consumer)

  (fan-out: two sinks subscribe to the same topic in different consumer groups)
```

### Threading model

| Component | Threads | Thread safety |
|---|---|---|
| `KafkaProducer` | One per `SourceRunner`, shared across source threads | Thread-safe |
| `Source` impl | Runs in one thread managed by `SourceRunner` | Impl-specific |
| `KafkaConsumer` | One per `SinkRunner` worker thread — never shared | NOT thread-safe |
| `Sink` impl | Shared across all `SinkRunner` worker threads | Must be thread-safe |
| DLQ `KafkaProducer` | One per `SinkRunner`, shared across worker threads | Thread-safe |

### Delivery semantics

- **Producer** — `acks=all` + `enable.idempotence=true` gives exactly-once delivery to the broker.
- **Consumer** — synchronous `commitSync()` after each poll batch gives at-least-once delivery. Document IDs `<topic>-<partition>-<offset>` make Elasticsearch re-indexing idempotent.
- **Elasticsearch writes** — the full poll batch is sent in a single Bulk API request. Per-item failure checking is performed only when a DLQ is configured.

---

## Features

- **SPI-based plugin architecture** — new source and sink types are registered in
  `META-INF/services/` and require no changes to the framework.
- **Multi-threaded sources and sinks** — thread counts are configured per instance in
  `pipeline.properties`; no code changes needed.
- **Flexible routing** — sources publish to topics; sinks subscribe to topics.
  Fan-out (one source to multiple sinks) and multiple isolated pipelines are both
  supported through standard Kafka consumer groups.
- **Bulk Elasticsearch writes** — entire poll batches are sent in a single HTTP request;
  per-item failure tracking is only enabled when a DLQ is configured.
- **Dead Letter Queue (DLQ)** — configurable per sink; failed records are routed to a
  dedicated Kafka topic for inspection and replay.
- **Single Message Transforms (SMTs)** — payload-only transform chain on both source and
  sink side, configured per instance via `transforms=<type1>,<type2>,...`. Built-in:
  `inject-record-timestamp` (reads the payload's Unix epoch `timestamp` and adds
  `recordTimestamp` as ISO-8601). A `null` return from any transform silently drops the
  record. Extend by implementing `Transform`, registering it in
  `META-INF/services/io.github.adityassharma.kafka.spi.Transform`.
- **Three message format modes** — JSON (default), Avro with file-based schema, Avro with
  Confluent Schema Registry.
- **Four Kafka security modes** — PLAINTEXT, SSL, SASL\_PLAINTEXT, SASL\_SSL; switched
  purely by properties.
- **Elasticsearch HTTPS + basic auth + JKS truststore** — all configured in
  `pipeline.properties`.
- **Daily rolling Elasticsearch index** — date-math index name `<iss-positions-{now/d{yyyy-MM-dd}}>`.
- **Graceful shutdown** — JVM shutdown hook stops all sources and sinks cleanly, flushes
  producers, commits final offsets, and closes all resources.
- **Built-in health & metrics endpoints** — optional lightweight HTTP server (JDK built-in,
  zero new dependencies) exposing `GET /health` (JSON component status) and `GET /metrics`
  (Prometheus plaintext consumer-lag gauges). Enabled by adding `management.port` to any
  properties file; works in all three deployment modes independently.

---

## Prerequisites

| Tool | Minimum version | Notes |
|---|---|---|
| Java (JDK) | 25 | `JAVA_HOME` must be set |
| Maven | 3.9 | Used to build and run tests |
| Apache Kafka | 4.2.0 | Broker + CLI scripts; KRaft mode (no ZooKeeper) |
| Elasticsearch | 8.x / 9.x | HTTPS recommended; HTTP also supported |
| Kibana | matching ES version | Optional — for visualization |
| Confluent Schema Registry | 7.x | Optional — Avro + Schema Registry mode only |

> **Local setup tip:** download the binary release from
> [kafka.apache.org](https://kafka.apache.org/downloads) and unzip it (e.g. to `C:\kafka`
> on Windows). Elasticsearch and Kibana can be downloaded individually from
> [elastic.co/downloads](https://www.elastic.co/downloads).

---

## Project Structure

```
kafka-ingest-pipeline/
├── config/
│   ├── pipeline.properties           # unified pipeline configuration
│   └── elasticsearch.truststore.jks  # local-only, excluded from git (*.jks)
├── src/
│   ├── main/java/.../kafka/
│   │   ├── spi/
│   │   │   ├── Source.java           # source plugin interface
│   │   │   ├── SourceContext.java    # framework handle passed to each source
│   │   │   ├── Sink.java             # sink plugin interface
│   │   │   ├── Record.java           # immutable record (topic/partition/offset/key/value/timestamp)
│   │   │   └── Transform.java        # SMT interface + loadChain() factory
│   │   ├── sources/
│   │   │   ├── IssApiSource.java     # polls Open-Notify ISS position API
│   │   │   ├── HttpListenerSource.java  # accepts HTTP POST (FluentBit etc.)
│   │   │   └── util/
│   │   │       └── DataFetcher.java  # HTTP GET helper used by polling sources
│   │   ├── sinks/
│   │   │   ├── ElasticsearchSink.java  # Bulk API writer with optional DLQ tracking
│   │   │   └── LoggingSink.java        # logs records via Log4j2
│   │   ├── transforms/
│   │   │   └── InjectRecordTimestamp.java  # SMT: adds recordTimestamp (ISO-8601) from payload epoch
│   │   ├── pipeline/
│   │   │   ├── PipelineMain.java     # entry point — discovers and starts all runners
│   │   │   ├── SourceRunner.java     # manages one source + its KafkaProducer
│   │   │   └── SinkRunner.java       # manages one sink + its consumer thread pool
│   │   ├── management/
│   │   │   ├── ComponentStatus.java  # enum: STARTING, RUNNING, STOPPED, ERROR
│   │   │   ├── SourceStats.java      # name, type, volatile status
│   │   │   ├── SinkStats.java        # name, type, volatile status, per-partition lag map
│   │   │   └── ManagementServer.java # JDK HttpServer: /health and /metrics
│   │   └── common/
│   │       ├── AppProperties.java        # loads & validates .properties files
│   │       ├── AvroConverter.java        # JSON <-> Avro GenericRecord
│   │       ├── AvroFileDeserializer.java # Kafka Deserializer<GenericRecord> (file schema)
│   │       ├── AvroFileSerializer.java   # Kafka Serializer<GenericRecord>   (file schema)
│   │       ├── KafkaClientFactory.java   # creates typed KafkaProducer/Consumer
│   │       ├── MessageFormat.java        # JSON / AVRO enum
│   │       └── SchemaLoader.java         # loads .avsc schema from file
│   └── resources/
│       └── META-INF/services/
│           ├── io.github.adityassharma.kafka.spi.Source     # registered Source impls
│           ├── io.github.adityassharma.kafka.spi.Sink        # registered Sink impls
│           └── io.github.adityassharma.kafka.spi.Transform   # registered Transform impls
├── src/test/java/.../kafka/
│   └── RoundTripIntegrationTest.java
├── pom.xml
└── README.md
```

---

## Setup

### Kafka

Kafka 4.x uses **KRaft mode** — ZooKeeper is no longer required or included.
Format storage once before the first start.

**1. Format storage (first time only)**

```bash
# Linux/macOS
bin/kafka-storage.sh random-uuid
bin/kafka-storage.sh format -t <uuid> -c config/server.properties --standalone

# Windows (from C:\kafka)
bin\windows\kafka-storage.bat random-uuid
bin\windows\kafka-storage.bat format -t <uuid> -c config\server.properties --standalone
```

**2. Start the broker**

```bash
# Linux/macOS
bin/kafka-server-start.sh config/server.properties

# Windows
bin\windows\kafka-server-start.bat config\server.properties
```

**3. Create topics**

```bash
# Main topic (Linux/macOS)
bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic open-notify-iss \
  --partitions 2 \
  --replication-factor 1

# DLQ topic (only needed if sink.es-iss.dlq.topic is set)
bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic open-notify-iss-es-iss-dlq \
  --partitions 1 \
  --replication-factor 1

# Windows — replace bin/kafka-topics.sh with bin\windows\kafka-topics.bat
```

**4. Verify**

```bash
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

**Check consumer lag at any time**

```bash
bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group open-notify-iss-consumer-group
```

---

### Elasticsearch & Kibana

**Start Elasticsearch**

```bash
bin/elasticsearch          # Linux/macOS
bin\elasticsearch.bat      # Windows
```

Listens on `https://localhost:9200` with security enabled by default.

**Start Kibana**

```bash
bin/kibana          # Linux/macOS
bin\kibana.bat      # Windows
```

Listens on `http://localhost:5601`.

**Reset the `elastic` user password**

```bash
bin/elasticsearch-reset-password -u elastic        # Linux/macOS
bin\elasticsearch-reset-password.bat -u elastic    # Windows
```

Update `pipeline.properties`:

```properties
sink.es-iss.elasticsearch.username=elastic
sink.es-iss.elasticsearch.password=your-password-here
```

**Set index replicas to 0** (single-node setup only):

```bash
curl -X PUT "https://localhost:9200/open-notify-iss/_settings" \
  -u elastic:<password> \
  -H "Content-Type: application/json" \
  -d '{"index":{"number_of_replicas":0}}'
```

---

### Truststore for Elasticsearch TLS

```bash
# The CA cert is at: <ES install dir>/config/certs/http_ca.crt
keytool -importcert \
  -alias elasticsearch-ca \
  -file /path/to/http_ca.crt \
  -keystore config/elasticsearch.truststore.jks \
  -storepass changeit \
  -noprompt
```

The `*.jks` file is excluded from version control. Each developer generates their own
from their local Elasticsearch instance.

---

## Configuration

All configuration lives in a single **`config/pipeline.properties`** file.

### pipeline.properties

#### Global properties (apply to all sources and sinks)

| Property | Default | Description |
|---|---|---|
| `bootstrap.servers` | *(required)* | Kafka broker address(es) |
| `message.format` | `json` | `json` or `avro` |
| `message.schema.file` | — | Avro file-based schema path |
| `message.schema.registry.url` | — | Confluent Schema Registry URL |
| `acks` | `1` | Producer acknowledgement level |
| `enable.idempotence` | — | `true` prevents duplicate messages on producer retry |
| `compression.type` | `none` | `snappy`, `lz4`, `gzip`, `zstd`, `none` |
| `security.protocol` | *(absent = PLAINTEXT)* | See [Kafka security modes](#kafka-security-modes) |
| `management.port` | *(absent = disabled)* | TCP port for `/health` and `/metrics`; omit to disable |

#### Source instance properties (`source.<name>.*`)

| Property | Description |
|---|---|
| `source.<name>.type` | Source type: `iss-api` or `http-listener` |
| `source.<name>.topic` | Kafka topic to publish to |
| `source.<name>.transforms` | Comma-separated SMT chain applied before producing (optional) |

**`iss-api` additional properties:**

| Property | Default | Description |
|---|---|---|
| `source.<name>.url` | *(required)* | HTTP endpoint to poll |
| `source.<name>.polling.interval.ms` | `5000` | Sleep between polls (ms) |

**`http-listener` additional properties:**

| Property | Default | Description |
|---|---|---|
| `source.<name>.port` | `8080` | TCP port to listen on |
| `source.<name>.path` | `/ingest` | URL path to accept POSTs on |
| `source.<name>.threads` | `4` | HTTP handler thread pool size |

#### Sink instance properties (`sink.<name>.*`)

| Property | Default | Description |
|---|---|---|
| `sink.<name>.type` | *(required)* | Sink type: `elasticsearch` or `logging` |
| `sink.<name>.topics` | *(required)* | Comma-separated Kafka topics to consume from |
| `sink.<name>.group.id` | *(required)* | Kafka consumer group ID (must be unique per sink) |
| `sink.<name>.threads` | `1` | Consumer worker threads (≤ partition count) |
| `sink.<name>.auto.offset.reset` | `earliest` | `earliest` or `latest` |
| `sink.<name>.dlq.topic` | *(absent = disabled)* | DLQ Kafka topic name; omit to disable DLQ |
| `sink.<name>.transforms` | *(absent = none)* | Comma-separated SMT chain applied before `writeBatch()` |

**`elasticsearch` additional properties:**

| Property | Default | Description |
|---|---|---|
| `sink.<name>.elasticsearch.host` | `localhost` | Elasticsearch hostname |
| `sink.<name>.elasticsearch.port` | `9200` | Elasticsearch port |
| `sink.<name>.elasticsearch.scheme` | `http` | `http` or `https` |
| `sink.<name>.elasticsearch.index` | *(required)* | Index name; date-math syntax supported |
| `sink.<name>.elasticsearch.username` | — | Basic auth username |
| `sink.<name>.elasticsearch.password` | — | Basic auth password |
| `sink.<name>.elasticsearch.ssl.truststore.location` | — | JKS truststore path |
| `sink.<name>.elasticsearch.ssl.truststore.password` | — | Truststore password |

**`logging` additional properties:**

| Property | Default | Description |
|---|---|---|
| `sink.<name>.log.level` | `INFO` | Log4j2 level: TRACE, DEBUG, INFO, WARN, ERROR |

---

### Routing: sources → topics → sinks

Routing is topic-based and works like standard Kafka:

```
source.iss-position.topic=open-notify-iss   → publishes to open-notify-iss
sink.es-iss.topics=open-notify-iss          → consumes from open-notify-iss
```

Sources write to topics; sinks subscribe to topics. The framework imposes no
additional routing layer.

### Fan-out: one source to multiple sinks

Point multiple sinks at the same topic, each with its **own consumer group ID**:

```properties
sinks=es-iss,log-iss

sink.es-iss.topics=open-notify-iss
sink.es-iss.group.id=open-notify-iss-es-consumer      # ← different group IDs

sink.log-iss.topics=open-notify-iss
sink.log-iss.group.id=open-notify-iss-log-consumer    # ← ensure independent offset tracking
```

Each consumer group receives all records independently — standard Kafka behaviour.

### Dead Letter Queue (DLQ)

Add `dlq.topic` to any sink to enable DLQ routing for that sink:

```properties
sink.es-iss.dlq.topic=open-notify-iss-es-iss-dlq
```

**When DLQ is enabled:**
- `ElasticsearchSink` inspects each item in the Bulk response and returns failed records.
- `SinkRunner` publishes each failed record (original JSON) to the DLQ topic.
- Offsets are committed after DLQ routing, so the main topic advances regardless.

**When DLQ is disabled** (property absent):
- `ElasticsearchSink` only checks the top-level `errors` boolean in the Bulk response (O(1)).
- Failures are logged and skipped.

Create the DLQ topic before starting the pipeline:

```bash
bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic open-notify-iss-es-iss-dlq \
  --partitions 1 \
  --replication-factor 1
```

### Single Message Transforms (SMTs)

SMTs let you transform a record's JSON payload before it is produced to Kafka (source-side)
or before it is written to the sink (sink-side). Transforms are applied in declaration order;
a transform that returns `null` silently drops the record (it is not sent to Kafka / written
to the sink, and is not routed to the DLQ).

Configure a comma-separated chain on any source or sink instance:

```properties
# source-side — applied before producing to Kafka
source.iss-position.transforms=inject-record-timestamp

# sink-side — applied before writeBatch()
sink.es-iss.transforms=inject-record-timestamp

# chained (left-to-right; null from any step drops the record)
sink.es-iss.transforms=inject-record-timestamp,mask-pii
```

**Important:** transforms operate on the JSON payload string only. Kafka metadata
(topic, partition, offset, key, timestamp) is **not** visible to transforms and is
passed through unchanged. This means:
- Source-side transforms apply before the record is produced, so the Kafka-assigned
  metadata is not yet available — only the raw payload.
- Sink-side transforms apply after consumption; the document ID
  `<topic>-<partition>-<offset>` is derived from the original metadata, so
  Elasticsearch re-indexing remains idempotent even after transformation.

#### Conversion order — where transforms fit

**Source side:**

```
source.emit(json)
  → applyTransformChain()          [transforms run here — JSON string in, JSON string out]
  → (JSON) ── message.format=json ──► producer.send()  → Kafka
  → (JSON) ── message.format=avro ──► AvroConverter.fromJson()  → GenericRecord
                                    → producer.send()  → Kafka (Avro binary)
```

**Sink side:**

```
Kafka
  → consumer.poll()
  → (String)       ── message.format=json ──► Record.value = raw JSON
  → (GenericRecord) ─ message.format=avro ──► AvroConverter.toJson()  → Record.value = JSON
  → applyTransforms()              [transforms run here — JSON string in, JSON string out]
  → sink.writeBatch()
```

**Avro constraint on the source side:** transforms run *before* `AvroConverter.fromJson()`
converts the JSON to a `GenericRecord`. That conversion is strict — any field in the JSON
that is not declared in the Avro schema throws an exception. Transforms that add new fields
(e.g. `inject-record-timestamp`) will fail on the source side unless the new field is
declared in the `.avsc` file. On the **sink side there is no such constraint** — the Avro
decode happens before transforms run, so the JSON is already free-form at that point.

#### Built-in transforms

| Type | Description |
|---|---|
| `inject-record-timestamp` | Reads `timestamp` (Unix epoch seconds) from the payload and adds `recordTimestamp` as an ISO-8601 string. If `timestamp` is absent or not numeric, the payload is forwarded unchanged. Never drops a record. |

#### Adding a custom transform

See [Adding a new Transform](#adding-a-new-source-sink-or-transform) below.

---

### Message format

| Value | Serialization | Extra properties required |
|---|---|---|
| `json` (default) | Raw JSON string | None |
| `avro` + `message.schema.file` | Avro binary, file-based schema | `message.schema.file=/path/to/schema.avsc` |
| `avro` + `message.schema.registry.url` | Avro binary, Schema Registry | `message.schema.registry.url=http://localhost:8081` |

### Kafka security modes

Uncomment exactly one block in `pipeline.properties`.

| Mode | `security.protocol` | Encryption | Authentication |
|---|---|---|---|
| 1 — Local dev (default) | *(absent)* | None | None |
| 2 — TLS only | `SSL` | TLS | None (one-way cert) |
| 3 — SASL, no TLS | `SASL_PLAINTEXT` | None | Username + password |
| 4 — SASL over TLS | `SASL_SSL` | TLS | Username + password |

**SASL\_SSL example:**

```properties
security.protocol=SASL_SSL
ssl.truststore.location=/path/to/client.truststore.jks
ssl.truststore.password=changeit
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="aditya" \
  password="aditya-secret";
```

### Health & Metrics

Add `management.port` to any properties file to enable the built-in HTTP server.
No new dependencies — it uses the JDK's `com.sun.net.httpserver.HttpServer`.

```properties
management.port=8081   # pipeline.properties / source.properties
management.port=8082   # sink.properties (different port when both run on the same host)
```

**`GET /health`** — JSON component status:

```json
{
  "status": "UP",
  "sources": [
    {"name": "iss-position", "type": "iss-api",       "status": "RUNNING"},
    {"name": "fluentbit",    "type": "http-listener",  "status": "RUNNING"}
  ],
  "sinks": [
    {"name": "es-iss",  "type": "elasticsearch", "status": "RUNNING"},
    {"name": "log-iss", "type": "logging",        "status": "RUNNING"}
  ]
}
```

Overall `status` is `UP` when all components are `RUNNING`, `ERROR` if any are in error,
or `STARTING` otherwise.

**`GET /metrics`** — Prometheus plaintext consumer-lag gauges:

```
# HELP kafka_consumer_lag Messages behind the latest offset
# TYPE kafka_consumer_lag gauge
kafka_consumer_lag{sink="es-iss",topic="open-notify-iss",partition="0"} 0
kafka_consumer_lag{sink="es-iss",topic="open-notify-iss",partition="1"} 0
```

Lag is sampled from the in-memory metadata piggy-backed on Kafka fetch responses —
no extra broker requests, zero polling overhead.  Gauges are populated after the first
successful `poll()` in each worker thread; until then no entries appear.

**Deployment note:** health and metrics work independently per JVM.  In the
separate-server deployment (Options 2 and 3), each JVM exposes its own endpoint
and only reports its own sources or sinks.

---

## Adding a new Source, Sink, or Transform

### New Source

1. Create a class in `sources/` that implements `io.github.adityassharma.kafka.spi.Source`.
2. Implement `type()` (unique string), `start(SourceContext)`, `stop()`, and `close()`.
3. Call `context.emit(topic, jsonString)` to publish a record. Kafka serialisation is handled by the framework.
4. Add the fully-qualified class name to `src/main/resources/META-INF/services/io.github.adityassharma.kafka.spi.Source`.
5. Add a source instance block to `pipeline.properties`.

### New Sink

1. Create a class in `sinks/` that implements `io.github.adityassharma.kafka.spi.Sink`.
2. Implement `type()`, `configure(Properties)`, `writeBatch(List<Record>)`, and `close()`.
3. In `writeBatch`: send the batch to your destination; return a list of failed `Record`s (empty if all succeeded). Check `props.getProperty("dlq.topic") != null` in `configure()` to decide whether per-item failure tracking is needed.
4. Ensure the implementation is **thread-safe** — a single instance is shared across all worker threads.
5. Add the fully-qualified class name to `src/main/resources/META-INF/services/io.github.adityassharma.kafka.spi.Sink`.
6. Add a sink instance block to `pipeline.properties`.

### New Transform

1. Create a class in `transforms/` that implements `io.github.adityassharma.kafka.spi.Transform`.
2. Implement three methods:
   - `type()` — unique kebab-case string used in `transforms=...` config (e.g. `"mask-pii"`).
   - `apply(String json)` — receive the JSON payload; return the transformed string, or `null` to drop the record.
   - `close()` — release any resources (no-op for stateless transforms).
3. Design for **thread safety** — a single instance may be called concurrently from multiple worker threads.
4. Add the fully-qualified class name to
   `src/main/resources/META-INF/services/io.github.adityassharma.kafka.spi.Transform`.
5. Reference it by type name in `pipeline.properties`:
   ```properties
   source.my-source.transforms=mask-pii
   sink.my-sink.transforms=inject-record-timestamp,mask-pii
   ```

**Example skeleton:**

```java
package io.github.adityassharma.kafka.transforms;

import io.github.adityassharma.kafka.spi.Transform;

public class MaskPii implements Transform {

    @Override public String type() { return "mask-pii"; }

    @Override
    public String apply(String json) {
        // manipulate JSON string; return null to drop the record
        return json.replaceAll("\"email\":\"[^\"]+\"", "\"email\":\"***\"");
    }

    @Override public void close() { /* stateless */ }
}
```

---

## Building

```bash
mvn clean package
```

Produces `target/kafka-ingest-pipeline-1.0.0-fat.jar` — a single uber-jar with all
dependencies bundled. The `ServicesResourceTransformer` in the Maven Shade Plugin
ensures `META-INF/services/` entries from all JARs are merged, so `ServiceLoader`
works correctly in the fat jar.

---

## Running

`PipelineMain` accepts a single argument — the path to a properties file.
The `sources` and `sinks` keys are both **optional**: if absent or empty the node simply
skips that side.  This supports three deployment configurations:

### Option 1 — single server, single JVM (combined)

```bash
# Linux/macOS
java -cp target/kafka-ingest-pipeline-1.0.0-fat.jar \
  io.github.adityassharma.kafka.pipeline.PipelineMain \
  config/pipeline.properties

# Windows
java -cp target\kafka-ingest-pipeline-1.0.0-fat.jar io.github.adityassharma.kafka.pipeline.PipelineMain config\pipeline.properties
```

### Option 2 — separate servers (source node + sink node)

Both nodes must point `bootstrap.servers` at the same Kafka cluster.

**Source node** (producer / ingestion server):
```bash
java -cp target/kafka-ingest-pipeline-1.0.0-fat.jar \
  io.github.adityassharma.kafka.pipeline.PipelineMain \
  config/source.properties
```

**Sink node** (consumer / indexing server):
```bash
java -cp target/kafka-ingest-pipeline-1.0.0-fat.jar \
  io.github.adityassharma.kafka.pipeline.PipelineMain \
  config/sink.properties
```

### Option 3 — same server, separate JVMs

Run the two commands above on the same machine to keep source and sink tuning
(heap size, GC settings) independent.

---

`PipelineMain` always starts sinks before sources to avoid dropping records published
before consumers are ready. Send `SIGINT` (Ctrl-C) to trigger graceful shutdown —
sources stop polling, in-flight messages are flushed, consumer workers finish their
current batch, commit offsets, and all resources are closed cleanly.

**Disable the FluentBit HTTP listener** (if port 8080 is unavailable or not needed):
remove `fluentbit` from the `sources=` list in the relevant properties file.

---

## Integration Test

`RoundTripIntegrationTest` produces 10 synthetic ISS-like messages to Kafka and verifies
that all 10 are received back, checking both count and content. It tests the raw Kafka
round-trip independently of the SPI layer.

**Prerequisites:**
- Kafka running on `localhost:9092`
- Topic `open-notify-iss` exists with at least 1 partition
- Elasticsearch is **not** required

```bash
mvn test -Pintegration
```

Unit tests are skipped by default (`skipTests=true` in `pom.xml`); the `integration`
Maven profile re-enables Surefire. The test uses `assign()` + `seekToEnd()` + `position()`
to avoid the asynchronous group-rebalance race condition.

---

## Elasticsearch Index & Kibana

### Index naming

The default index uses Elasticsearch date-math syntax:

```
<iss-positions-{now/d{yyyy-MM-dd}}>
```

This resolves to a daily index such as `iss-positions-2026-05-12`.  To use a static name:

```properties
sink.es-iss.elasticsearch.index=iss-positions
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
| `recordTimestamp` | `date` | ISO-8601 version of `timestamp`, added before indexing |
| `iss_position.latitude` | `text` | ISS latitude |
| `iss_position.longitude` | `text` | ISS longitude |
| `message` | `text` | API status (`"success"`) |

---

## Troubleshooting

**No Source implementation found for type='...'**
- Check that the type value in `pipeline.properties` exactly matches the string returned
  by `Source.type()` in your implementation.
- Confirm the class is listed in
  `src/main/resources/META-INF/services/io.github.adityassharma.kafka.spi.Source`.
- Rebuild the fat jar — `ServicesResourceTransformer` must run to merge service files.

**No Transform implementation found for type='...'**
- Confirm the type name in `transforms=...` exactly matches the string returned by
  `Transform.type()` in your implementation.
- Confirm the class is listed in
  `src/main/resources/META-INF/services/io.github.adityassharma.kafka.spi.Transform`.
- Rebuild the fat jar so `ServicesResourceTransformer` merges the service file.

**Sink receives no messages**
- Confirm `sink.<name>.topics` matches the topic the source publishes to.
- Check `auto.offset.reset=earliest` if you want to replay messages already in the topic.
- Verify `sink.<name>.threads` is not greater than the number of topic partitions — excess
  threads receive no partition assignment and sit idle.

**Port 8080 already in use**
- Either change `source.fluentbit.port` or remove `fluentbit` from the `sources=` list.

**`ConfigException: Must set acks to all in enable.idempotence=true`**
- Set `acks=all` in `pipeline.properties`.

**`Connection is closed` or SSL handshake errors (Elasticsearch)**
- Confirm `elasticsearch.scheme=https` in `pipeline.properties`.
- Confirm the JKS truststore contains the Elasticsearch CA certificate.

**Yellow Elasticsearch index status**
- Single-node clusters cannot satisfy the default replica count of 1. Set replicas to 0
  (see [Elasticsearch & Kibana setup](#elasticsearch--kibana)).

**DLQ topic does not exist**
- Create the DLQ topic before starting the pipeline, or set
  `auto.create.topics.enable=true` on the Kafka broker (not recommended for production).

**Check consumer lag**
```bash
bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group open-notify-iss-consumer-group
```

**Integration test receives 0 messages**
- Confirm the topic exists:
  ```bash
  bin/kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic open-notify-iss
  ```

**`/health` returns `STARTING` immediately after launch**
- Sources and sinks transition to `RUNNING` as their threads start.
  Allow a few seconds after the pipeline log line `Pipeline running: N source(s), N sink(s)`.

**Port conflict on `management.port`**
- Use different ports when two JVMs run on the same host
  (e.g. `8081` in `source.properties` and `8082` in `sink.properties`).
  Omit `management.port` entirely to disable the endpoint with zero overhead.
