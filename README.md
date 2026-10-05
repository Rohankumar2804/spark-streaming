# Kafka + Avro + Schema Registry + Spark, locally

A small lab with real Kafka messages and native Spark Avro decoding. No cloud
account or paid service is needed. Kafka and Spark are Apache projects; the local
Confluent Schema Registry image is free to use under its applicable license.
This is a production-style data contract demonstration, using a single broker
and a local SQLite JSON sink with console logging for learning. It is not a highly available deployment.

## Start here

You need Docker Engine with Docker Compose v2 (or Docker Desktop with your WSL
distro enabled). Allow roughly 6 GB of Docker memory and several GB of disk.
The first build needs internet to download images, Python packages, and Spark
connector JARs. Python 3.11, Java 17, and Spark 3.5.7 run inside the app container.

Run these commands from this directory:

```bash
docker compose up -d --wait kafka schema-registry
docker compose build app
docker compose run --rm app python setup_demo.py
docker compose ps
```

Setup creates three-partition topics, configures `BACKWARD_TRANSITIVE` compatibility
per subject, and registers payment v1, payment v2, refund, and envelope schemas.
It is safe to rerun. Registering an identical schema does not create another
version. Schema IDs are assigned by Registry; never assume the example IDs below
are the IDs on your machine.

If you already ran the earlier two-partition version, rerun `setup_demo.py`: it
increases each topic to three partitions while preserving existing messages.
Existing records stay in their original partitions. Increasing partition count
can change the default key-to-partition mapping for future records, so historical
ordering for a key across that change is not guaranteed. Use new checkpoint paths
for the replay examples below.

## 1. One topic per event type

```bash
docker compose run --rm app python producer.py --mode separate --payment-version v1
docker compose run --rm app sh scripts/spark.sh --mode separate --available-now
```

One payment goes to `payments`; one refund goes to `refunds`. Kafka gives Spark
binary `value` plus topic, partition, and offset. The consumer reads the schema
ID, retrieves its writer schema, removes the header, and calls `from_avro`.
You see two different typed event structs in the batch output. The v2 reader is
the default, so a v1 payment has `payment_method: null` after decoding.

To leave a streaming consumer running, omit `--available-now`, then produce more
events in a second terminal. Stop it with Ctrl-C. `--count 5` produces five
payment/refund pairs **per selected payment version**; `both` is the default.

## 2. Different event types in one topic

```bash
docker compose run --rm app python producer.py --mode shared --payment-version both
docker compose run --rm app python inspect_message.py --topic billing-events --count 4
docker compose run --rm app sh scripts/spark.sh --mode shared --available-now
```

This sends v1 payment, refund, v2 payment, refund to `billing-events`. Broker
partition ordering is preserved; no total order is promised across partitions.
Payment/refund pairs use the payment event ID as the Kafka key so each pair shares
a partition on this topic. The key is a plain UTF-8 string; only the value is Avro.

The producer explicitly follows these subject naming conventions:

| Topic | Registry subject | Versions |
|---|---|---|
| payments | payments-value | PaymentCreated v1, v2 |
| refunds | refunds-value | RefundCreated |
| billing-events | billing-events-demo.billing.PaymentCreated | PaymentCreated v1, v2 |
| billing-events | billing-events-demo.billing.RefundCreated | RefundCreated |
| billing-envelopes | billing-envelopes-value | BillingEnvelope |

The shared topic uses `TopicRecordNameStrategy` naming. Compatibility is checked
independently for each record type in that topic. Trying to register two unrelated
records under `billing-events-value` with backward compatibility would fail.
These conventions are implemented explicitly in the Python code; there is no
hidden serializer setting. [Subject naming documentation](https://docs.confluent.io/platform/7.9/schema-registry/fundamentals/serdes-develop/index.html).

**How does Spark pick a schema?** Each message identifies its **writer schema**.
`route_batch` groups a micro-batch by schema ID, fetches each schema once on the
driver, and identifies `demo.billing.PaymentCreated` or
`demo.billing.RefundCreated` from its full record name. It builds a separate
`from_avro` expression for each group, with that exact writer schema and an
explicit reader schema. The ID is authoritative; the topic alone is insufficient.
An unknown event type stops the query instead of being silently dropped.

**The Spark limitation:** a DataFrame has one known schema at query planning
time. `from_avro` takes a schema JSON string, not a different schema column per
row. You cannot have a single `event` column whose structure changes arbitrarily
for each Kafka record. Applying the payment schema to refund bytes can fail or,
worse, produce plausible incorrect values. Schema Registry does not remove this
constraint. [Spark Avro API and reader-schema options](https://spark.apache.org/docs/3.5.7/sql-data-sources-avro.html).

Here the shared streaming DataFrame contains raw bytes and metadata. The typed
payment/refund DataFrames exist **inside `foreachBatch`**; they are batch
DataFrames, not one dynamically typed streaming DataFrame. You can write them to
separate typed tables. For a unified table, explicitly normalize to a known
superset or envelope. Arbitrarily merging unrelated records is not automatic
schema evolution. Registry requests are cached by ID and happen on the driver,
not once per row on Spark workers. This demo caps each batch at 32 distinct IDs.

## 3. A common, typed event envelope

```bash
docker compose run --rm app python producer.py --mode envelope --payment-version v2
docker compose run --rm app sh scripts/spark.sh --mode envelope --available-now
```

This uses `billing-envelopes` to keep the raw shared-topic lesson separate. A
payment looks like this after deserialization:

```json
{
  "event_id": "generated-uuid",
  "event_type": "PAYMENT_CREATED",
  "timestamp": 1791187200000,
  "payload": {
    "payment": {
      "user_id": "user-42",
      "amount": 49.95,
      "currency": "USD",
      "payment_method": "card"
    },
    "refund": null
  }
}
```

Both event types use one envelope Avro schema. `timestamp` is Unix milliseconds.
`payload` has two nullable typed fields; exactly one must match `event_type`.
The producer constructs that contract and Spark checks it. The resulting
streaming DataFrame has these stable columns before the sink:

```text
topic: string, partition: int, offset: long, schema_id: long
event_id: string, event_type: string, timestamp: long
payload: struct<
  payment: struct<user_id:string, amount:double, currency:string, payment_method:string>,
  refund: struct<payment_id:string, refund_amount:double, reason:string>>
```

For example, before `writeStream` in `spark_consumer.py`, you can use
`events.filter("event_type = 'PAYMENT_CREATED'").select("event_id", "payload.payment.*")`.
This is a genuinely typed streaming DataFrame. Wrapping arbitrary bytes or an
unstructured JSON string in an envelope would still leave the inner decoding
problem unresolved. This choice of typed slots suits a small bounded event family;
many unrelated event types may be better split into separate topics/tables.

The example pins one envelope writer schema and rejects unknown IDs. Adding a
new event slot should use a nullable field with a default and a planned consumer
deployment; the query still needs explicit handling for any new writer versions.
`--payment-version` in envelope mode changes sample payload content only; it does
not change the envelope schema ID. The payment v1/v2 evolution exercise below uses
the standalone PaymentCreated schemas.

## 4. Schema evolution: old and new consumers

Payment v1 and v2 have the **same Avro full name**. V2 adds:

```json
{"name": "payment_method", "type": ["null", "string"], "default": null}
```

Nullable allows a null value; the default lets a reader supply a value when an
older writer did not have that field. Those are different responsibilities.

```bash
docker compose run --rm app python producer.py --mode separate --payment-version both

# Distinct checkpoints let both readers consume the same historical messages.
docker compose run --rm app sh scripts/spark.sh --mode separate --payment-reader v1 --checkpoint /checkpoints/evolution-old --available-now
docker compose run --rm app sh scripts/spark.sh --mode separate --payment-reader v2 --checkpoint /checkpoints/evolution-new --available-now
```

| Writer | Reader | Observed payment_method |
|---|---|---|
| v1 | v1 | Column absent |
| v1 | v2 | null, supplied by the reader default |
| v2 | v1 | Column absent; reader ignores the additional field |
| v2 | v2 | card |

The consumer always gets the actual writer schema by ID, then passes its desired
reader schema as Spark's `avroSchema` option. Using the latest schema as the
writer schema for all historical bytes is incorrect.

`BACKWARD_TRANSITIVE` checks that a new reader schema can read data written with
**every previous schema in the subject**. `BACKWARD` checks only the preceding
version. Neither setting promises that old readers can read every possible future
writer. This particular optional-field addition works in both directions; a
policy requiring both directions across history is `FULL_TRANSITIVE`.
An old reader ignores the new field only when proper writer/reader resolution is
used. It is not generally safe to decode v2 bytes as though v1 wrote them.
[Compatibility documentation](https://docs.confluent.io/platform/7.7/schema-registry/fundamentals/schema-evolution.html).

Try an incompatible change without altering stored schemas:

```bash
docker compose run --rm app python check_compatibility.py
```

It asks Registry whether adding a required field without a default is compatible,
asserts the answer is false, and confirms that registration is rejected with HTTP
409. A rejected registration does not add a subject version.

## Inspect Registry and actual Kafka bytes

Run these on your host (no `jq` required):

```bash
curl -fsS http://localhost:8081/subjects
curl -fsS http://localhost:8081/subjects/payments-value/versions
curl -fsS http://localhost:8081/subjects/payments-value/versions/1
curl -fsS http://localhost:8081/subjects/payments-value/versions/2
curl -fsS http://localhost:8081/config/payments-value
curl -fsS http://localhost:8081/subjects/billing-events-demo.billing.PaymentCreated/versions
```

Copy an `id` from one of those responses (or a producer log), then substitute it:

```bash
curl -fsS http://localhost:8081/schemas/ids/1
docker compose run --rm app python inspect_message.py --topic payments --count 1
```

The inspector reads **actual broker messages**, logs their header and payload,
retrieves the schema by ID, and prints the decoded event with partition and offset.
A fresh inspection group starts from earliest and does not commit offsets.

```text
Kafka value bytes:
+----------+----------------------+------------------------------+
| byte 0   | bytes 1..4           | bytes 5 onward               |
| 0x00     | schema ID, big endian| raw Avro binary datum        |
+----------+----------------------+------------------------------+

Example header_hex=0000000001 means magic=0, schema_id=1.
```

The five-byte prefix has no schema JSON, field names, record name, subject, or
subject version. The Avro payload holds values encoded according to the writer
schema; strings such as `PAYMENT_CREATED` can appear because they are data.
Schema JSON is stored in Registry (persisted in Kafka's internal `_schemas`
topic), not copied into each business message. This is raw Avro datum framing,
not an Avro object container file, which *does* embed a schema in its file header.
[Confluent Avro serialization](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-avro.html).

`schema_id` identifies schema content in this Registry; `version` is a position
within a subject's history. The same schema can reuse an ID across subjects.
Kafka offsets identify positions within partitions and have no relationship to
schema IDs or subject versions.

## Save decoded events as JSON in SQLite

Every consumer mode now writes decoded events into `data/events.db`, visible in
your project through the existing `/app` bind mount. Python includes SQLite, so
no extra package or service is needed. Select another file with `--sqlite-path`.

```bash
# A NEW checkpoint replays records that the previous console-only consumer already read.
docker compose run --rm app sh scripts/spark.sh --mode envelope --available-now --checkpoint /checkpoints/sqlite-envelope-1
docker compose run --rm --no-deps app python inspect_sqlite.py --limit 10

# Shared topic: fetch each writer schema by ID, decode, then save the JSON.
docker compose run --rm app sh scripts/spark.sh --mode shared --available-now --checkpoint /checkpoints/sqlite-shared-1
```

The `events` table contains `topic`, `kafka_partition`, `kafka_offset`, `schema_id`,
`event_id`, `event_type`, `event_json`, and `batch_id`. `event_json` is validated
JSON text containing the decoded event; envelopes retain their nested payload.
V1 payments read with the v2 reader retain `"payment_method": null` in JSON.
Kafka metadata lives in separate columns. You can open `data/events.db` in a
SQLite viewer or query it with:

```sql
SELECT topic, kafka_partition, kafka_offset, event_type, event_json FROM events;
SELECT event_type, COUNT(*) FROM events GROUP BY event_type;
SELECT event_id, json_extract(event_json, '$.payment_method') FROM events
WHERE event_type = 'PAYMENT_CREATED' AND topic = 'payments';
```

The decoding call is in `decode_frame()`:

```python
from_avro(
    F.col("avro_payload"),  # Kafka value after removing its five-byte header
    json.dumps(writer),
    {"avroSchema": json.dumps(reader), "mode": "FAILFAST"}
)
```

The flow is Kafka bytes → writer-schema lookup → Avro decoding → typed DataFrame
→ JSON → SQLite. Decoding/logging tasks still run in parallel. After those tasks,
cached decoded rows are streamed to one driver-side SQLite connection, with all
schema groups committed together in one micro-batch transaction. SQLite writes
are intentionally serial: this is a small local sink, not a distributed database.

The primary key `(topic, kafka_partition, kafka_offset)` makes retries/replays
idempotent: an already stored record is skipped using a targeted
`ON CONFLICT ... DO NOTHING`. Other integrity errors still fail and roll back
the batch. [SQLite conflict handling](https://www.sqlite.org/lang_upsert.html).
`SQLITE_COMMITTED` logs the number of newly inserted rows after commit succeeds.
If the process crashes after SQLite commits but before Spark checkpoints, replay
will skip the existing rows. Worker logs can still repeat. `batch_id` records the
batch that first inserted the row; it is not globally unique across queries.

The first stored reader representation wins. To compare v1/v2 reader outputs,
use separate `--sqlite-path data/old-reader.db` and `data/new-reader.db` files as
well as separate checkpoints. If Kafka topics are deleted/recreated or you switch
clusters, use a new database and checkpoint because Kafka coordinates can be reused.
Deleting the database alone does not reset Spark's checkpoint; use a new checkpoint
to rebuild it from retained Kafka data. `data/` is excluded from Git and image builds.

## Checkpoints, failures, and realistic limits

- Spark stores progress in the project's `.checkpoints/` directory, bind-mounted
  at `/checkpoints` inside the app container. For example,
  `--checkpoint /checkpoints/test-parallel-1` writes to
  `.checkpoints/test-parallel-1/` on your host, where you can open `metadata`,
  `offsets/0`, and `commits/0` directly in your IDE. The folder is excluded from
  Git and Docker build context. Checkpoints from the previous named Docker volume
  are not automatically copied here; that volume is left intact. A query using
  an empty local checkpoint directory starts from the earliest retained records.
  Restarting with
  the same checkpoint resumes its offsets. `startingOffsets=earliest` applies
  only to a new checkpoint. Use a new checkpoint directory to replay. Do not run
  two queries concurrently against the same checkpoint or reuse it after changing
  source topics/query structure.
- `foreachBatch` plus console logging has **at-least-once side effects**. A crash
  after printing but before checkpoint commit can print the same rows again.
  Checkpointing alone does not make arbitrary sinks exactly once. A real sink
  needs transactional/idempotent writes, e.g. deduplication by stable query identity
  and batch ID, or topic/partition/offset. Business deduplication by event ID is a
  separate concern. [Spark foreachBatch guarantees](https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html).
- Producer idempotence reduces duplicates from broker retries within a producer
  session. Rerunning this producer creates new UUIDs and intentionally new events;
  it is not an end-to-end exactly-once pipeline.
- On a Registry outage, a running consumer can keep using cached schemas. An
  unknown ID or a fresh process requiring a lookup fails after finite retries.
  The failed batch is not marked complete and is replayed after a restart. The
  envelope query fetches its pinned schema at startup and then requires no Registry
  calls for that version. A running producer can use its cached IDs; a fresh demo
  producer requires Registry for exact-schema lookup. Kafka itself can still
  store bytes during a Registry outage.
- Try the outage behavior after producing messages: `docker compose stop
  schema-registry`, then `docker compose run --rm --no-deps app python
  inspect_message.py --topic payments --count 1`. The fresh inspector fails to
  resolve the ID. Restore it with `docker compose start schema-registry`.
- Invalid headers, tombstones, unknown event types, and Avro decoding errors fail
  the query visibly. A production application could quarantine them with their
  original bytes and metadata; silently filtering them would hide data loss.
- There is one broker, replication factor one, local checkpoints, plaintext
  networking, and a local SQLite sink with worker logging. Money uses `double` because the task
  asks for it; financial systems commonly choose decimal or integer minor units.
  The default Kafka retention still applies: this is not an archival store.

## See three partitions processing in parallel

The Spark launcher uses `local[3]`: three task slots in one local Spark process.
For the clearest demonstration, use the envelope topic, which has three Kafka
partitions and one common schema. Produce 18 events (nine payment/refund pairs)
evenly across all three partitions:

```bash
docker compose run --rm app python setup_demo.py
docker compose run --rm app python producer.py --mode envelope --payment-version v2 --count 9 --round-robin
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic billing-envelopes

# Drain the available backlog in micro-batches, then exit.
docker compose run --rm app sh scripts/spark.sh --mode envelope --available-now --max-offsets-per-trigger 6 --task-delay-seconds 2 --checkpoint /checkpoints/parallel-available

# Replay with exactly one micro-batch, then exit.
docker compose run --rm app sh scripts/spark.sh --mode envelope --once --checkpoint /checkpoints/parallel-once
```

`--round-robin` explicitly assigns each pair to partition 0, 1, then 2, repeating.
Both events in a pair use the same partition. Without this option, the producer
uses the normal key-based partitioner. Explicit assignment is just a teaching
aid to guarantee that a small sample exercises all three partitions.

With only these 18 events in the topic and a fresh checkpoint, the AvailableNow
example processes three micro-batches of six events. An existing backlog adds
more work. The offset budget applies across the whole subscription, not per
partition; allocation follows each partition's backlog and rounding can affect
batch sizes. A reused checkpoint resumes instead of replaying.

Watch these logs:

- `BATCH_START` / `BATCH_END`: micro-batch ID and elapsed time.
- `TASK_START`: batch ID, stage ID, Spark partition ID, task attempt ID, worker
  process ID, and start time in epoch seconds.
- `DECODED`: those task details plus the event, schema ID, Kafka topic, Kafka
  partition, and offset inside `record`.
- `TASK_END`: the task's measured end time, row count, and Kafka partitions read.
  Summaries print on the driver after the job finishes; compare their recorded
  timestamps, not the order in which the summary lines appear.

For three tasks in the same stage, if the latest `started_at` is earlier than
the earliest `ended_at`, all three overlapped. The optional two-second pause in
each nonempty task makes this observable even for tiny inputs; leave it at its
default zero for ordinary processing. It demonstrates concurrent task execution,
not a throughput benchmark. Worker event logs can interleave.

Kafka partition IDs and Spark task partition IDs are separate identifiers.
With this query's default Kafka source mapping and no repartition, a populated
Kafka partition supplies a Spark input partition. Three local slots allow three
tasks to run concurrently; empty partitions may have no work. Separate mode reads
two topics, so there can be six Kafka input partitions, scheduled across three
slots. Shared mode also handles its schema groups sequentially within a batch,
while tasks within each group can run in parallel. `spark.sql.shuffle.partitions=3`
controls shuffle output; it does not set Kafka's partition count or task slots.
[Spark Kafka partition mapping](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html).

| Trigger flag | Behavior |
|---|---|
| `--available-now` | Processes the backlog captured at query startup in one or more micro-batches, respecting the offset budget, then exits. |
| `--once` | Processes available data in a single micro-batch, then exits. This demo omits the offset limit for this trigger because Spark ignores it. |
| Neither | Runs successive micro-batches on a five-second processing-time trigger until stopped. |

Both finite triggers still use Structured Streaming and checkpoints. Micro-batches
run sequentially; parallelism occurs among tasks **within** a batch. `Once` is
deprecated in Spark 3.5.7; prefer `AvailableNow` for bounded runs. The flags are
mutually exclusive. [Spark trigger migration guidance](https://spark.apache.org/docs/3.5.7/ss-migration-guide.html).

## Tests and troubleshooting

```bash
# No running services needed for these two tests.
docker compose run --rm --no-deps app python -m unittest discover -s tests -p 'test_*.py' -v
docker compose run --rm --no-deps -e PYTHONPATH=/app app spark-submit --master 'local[3]' --packages org.apache.spark:spark-avro_2.12:3.5.7 tests/spark_checks.py

# Service-backed smoke run: setup, compatibility, production, inspection, all consumers.
sh scripts/smoke.sh
```

The tests cover framing, both event types, all four payment writer/reader pairs,
Registry caching, Spark Avro decoding, envelope validation, unknown IDs, and
overlapping execution of three nonempty Spark tasks in different worker processes,
SQLite JSON output in every mode, replay deduplication, and transaction rollback.
Expected failure tests print Spark error messages before the final PASS line.

If Spark cannot download packages, check internet/proxy access to Maven Central.
The `ivy-cache` volume avoids repeat downloads. If the consumer prints nothing,
its checkpoint may already have consumed the data: produce more or select a new
checkpoint. Host tools use `localhost:9092` and `localhost:8081`; containers use
`kafka:29092` and `schema-registry:8081`. Do not interchange these addresses.

```bash
docker compose logs --tail=100 kafka schema-registry
docker compose down           # stop while preserving data/checkpoints
docker compose down -v        # DELETE Kafka data and Ivy cache; local .checkpoints remains
```

After deleting Kafka data, use a new checkpoint directory for the recreated topics.

## Interview-friendly execution flow

1. **Producer:** Python builds a payment or refund dictionary and selects the
   writer schema. Setup has already registered that schema.
2. **Avro serialization + Registry:** the producer looks up the schema's ID and
   encodes values with that schema. It puts magic byte zero and the four-byte ID
   before the Avro bytes. Registry stores contracts; it is not in the event data
   path between producer and Kafka. Registration/ID lookup happens before sending.
3. **Kafka:** stores the key and value bytes and assigns a partition offset. Kafka
   does not decode our Avro or know the business fields.
4. **Spark Structured Streaming:** reads Kafka bytes in micro-batches and tracks
   consumed offsets in its checkpoint.
5. **Avro deserialization:** extracts the ID, gets the writer schema from Registry
   or cache, removes the five-byte prefix, and resolves values against the chosen
   reader schema.
6. **Structured DataFrame:** Spark exposes typed columns for transformations.
   Unrelated shared-topic records need explicit routing; the common envelope gives
   the streaming query one stable nested structure.

## Ten interview questions

1. How do Avro and JSON differ in message size, readability, and schema enforcement?
2. What does Schema Registry store, and what does Kafka store in this project?
3. What are the magic byte and schema ID? How is an ID different from a subject
   version or Kafka offset?
4. Why does a v2 reader return null when reading a v1 payment, and why does a v1
   reader omit payment_method when reading a v2 payment?
5. How do BACKWARD, BACKWARD_TRANSITIVE, and FULL_TRANSITIVE compatibility differ?
6. Why does the shared topic use TopicRecordNameStrategy, and how does the consumer
   determine the writer schema for each record?
7. What goes wrong if you pass the complete Kafka value directly to from_avro,
   or always decode with the latest writer schema?
8. Why can't one Spark DataFrame change its schema per row, and how does the typed
   envelope solve that problem?
9. What does a Spark checkpoint guarantee here? What else would be required for
   exactly-once writes to an external database?
10. What happens when Schema Registry becomes unavailable for a warm consumer,
    a new consumer, and a producer encountering a new schema?
