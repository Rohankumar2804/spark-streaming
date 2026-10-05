"""Native Spark Avro decoding with explicit writer-schema routing and checkpoints."""

import argparse
from functools import partial
import json
import logging
import math
import os
import time

from pyspark import TaskContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.avro.functions import from_avro

from common import BOOTSTRAP, TOPICS, Registry, configure_logging, fullname, schema
from sqlite_sink import batch_transaction, insert_events

META = ["topic", "partition", "offset", "schema_id"]


def wire_columns(df):
    # Spark substring positions are 1-based. Preserve Kafka metadata alongside the bytes.
    valid = F.col("value").isNotNull() & (F.length("value") >= 5) & (
        F.hex(F.substring("value", 1, 1)) == "00")
    # Fail loudly for tombstones/invalid headers, never silently discard records.
    checked = df.withColumn("wire", F.when(valid, F.col("value")).otherwise(
        F.raise_error("Invalid Confluent Avro header or tombstone").cast("binary")))
    return (checked.withColumn("schema_id", F.conv(F.hex(F.substring("wire", 2, 4)), 16, 10).cast("long"))
            .withColumn("avro_payload", F.expr("substring(wire, 6, length(wire) - 5)")))


def decode_frame(df, writer, reader):
    # Actual writer schema interprets bytes. Reader schema controls the resulting columns.
    return df.select(*META, from_avro(
        F.col("avro_payload"),  # Binary datum after removing magic byte + schema ID.
        json.dumps(writer),
        {"avroSchema": json.dumps(reader), "mode": "FAILFAST"}
    ).alias("event"))


def log_partition(rows, batch_id, task_delay_seconds=0):
    """Runs on a Spark worker; return one small timing summary per task."""
    context = TaskContext.get()
    details = {"batch_id": batch_id, "stage_id": context.stageId(),
               "spark_partition_id": context.partitionId(), "task_attempt_id": context.taskAttemptId(),
               "attempt": context.attemptNumber(), "worker_pid": os.getpid(), "started_at": time.time()}
    print("TASK_START " + json.dumps(details), flush=True)
    count, kafka_partitions = 0, set()
    for row in rows:
        if count == 0 and task_delay_seconds:
            # Optional teaching aid: keep active tasks visible long enough to compare.
            time.sleep(task_delay_seconds)
        print("DECODED " + json.dumps({**details, "record": json.loads(row.json)}), flush=True)
        kafka_partitions.add((row.topic, row.partition))
        count += 1
    yield {**details, "ended_at": time.time(), "rows": count,
           "kafka_partitions": sorted(kafka_partitions)}


def log_rows(df, batch_id, task_delay_seconds=0):
    df.printSchema()
    rendered = df.select("topic", "partition",
                         F.to_json(F.struct("*"), {"ignoreNullFields": "false"}).alias("json"))
    # One Spark job schedules all partitions together. Collect only task summaries;
    # event logging happens on workers, avoiding sequential toLocalIterator jobs.
    summaries = rendered.rdd.mapPartitions(partial(log_partition, batch_id=batch_id,
                                                  task_delay_seconds=task_delay_seconds)).collect()
    for summary in summaries:
        logging.info("TASK_END %s", json.dumps(summary))


def route_batch(batch, batch_id, registry, payment_reader, mode, task_delay_seconds=0, on_decoded=None):
    # foreachBatch is the boundary at which each schema can have its own batch DataFrame.
    # Registry access stays on the driver and is cached by ID, not called per event.
    batch.persist()
    try:
        ids = [row.schema_id for row in batch.select("schema_id").distinct().limit(33).collect()]
        if len(ids) > 32:
            raise ValueError("Too many writer schemas in one demo batch (limit 32)")
        for schema_id in sorted(ids):
            writer = registry.get(schema_id)
            record = fullname(writer)
            if record == "demo.billing.PaymentCreated":
                event_type, reader, expected_topic = "PAYMENT_CREATED", schema(f"payment_{payment_reader}"), "payments"
            elif record == "demo.billing.RefundCreated":
                event_type, reader, expected_topic = "REFUND_CREATED", schema("refund"), "refunds"
            else:
                raise ValueError(f"Unhandled schema_id={schema_id} record={record}; deploy a handler")
            subset = batch.filter(F.col("schema_id") == schema_id)
            if mode == "separate" and subset.filter(F.col("topic") != expected_topic).limit(1).count():
                raise ValueError(f"Unexpected {record} outside {expected_topic}")
            decoded = decode_frame(subset, writer, reader).withColumn("event_type", F.lit(event_type))
            (on_decoded or log_rows)(decoded, batch_id, task_delay_seconds)
    finally:
        batch.unpersist()


def envelope_frame(raw, registry):
    reader = schema("envelope")
    expected_id = registry.lookup("billing-envelopes-value", reader)
    writer = registry.get(expected_id)
    # This example deliberately pins one envelope writer. Unknown versions must get
    # an explicit migration; do not decode them using a guessed/latest schema.
    checked = raw.withColumn("avro_payload", F.when(F.col("schema_id") == expected_id,
        F.col("avro_payload")).otherwise(F.raise_error("Unknown envelope schema ID; deploy reader migration").cast("binary")))
    decoded = decode_frame(checked, writer, reader)
    # Avro validates the field types. This additional check validates the event contract.
    payment = ((F.col("event.event_type") == "PAYMENT_CREATED") &
               F.col("event.payload.payment").isNotNull() & F.col("event.payload.refund").isNull())
    refund = ((F.col("event.event_type") == "REFUND_CREATED") &
              F.col("event.payload.refund").isNotNull() & F.col("event.payload.payment").isNull())
    decoded = decoded.withColumn("event", F.when(payment | refund, F.col("event")).otherwise(
        F.raise_error("event_type must match exactly one envelope payload")))
    return decoded.select(*META, "event.*")


def save_batch(batch, batch_id, registry, payment_reader, mode, database, task_delay_seconds=0):
    """Decode/log in Spark, then store JSON on the driver in one SQLite transaction."""
    inserted = 0
    with batch_transaction(database) as connection:
        def save_decoded(decoded, current_batch_id, delay):
            nonlocal inserted
            decoded.persist()
            try:
                # This action still schedules worker tasks concurrently, as before.
                log_rows(decoded, current_batch_id, delay)
                # Separate/shared modes have an event struct; envelope fields are flattened.
                event = F.col("event") if "event" in decoded.columns else F.struct(
                    *[F.col(name) for name in decoded.columns if name not in META])
                event_id = F.col("event.event_id") if "event" in decoded.columns else F.col("event_id")
                records = decoded.select(*META, event_id.alias("event_id"), "event_type",
                                         F.to_json(event, {"ignoreNullFields": "false"}).alias("event_json"))
                # SQLite is a local, single-writer sink. Stream cached rows to the driver;
                # never share a SQLite connection across Spark worker processes.
                rows = ((row.topic, row.partition, row.offset, row.schema_id, row.event_id,
                         row.event_type, row.event_json, current_batch_id) for row in records.toLocalIterator())
                inserted += insert_events(connection, rows)
            finally:
                decoded.unpersist()

        if mode == "envelope":
            save_decoded(batch, batch_id, task_delay_seconds)
        else:
            route_batch(batch, batch_id, registry, payment_reader, mode, task_delay_seconds, save_decoded)
    logging.info("SQLITE_COMMITTED batch_id=%s inserted=%s database=%s", batch_id, inserted, database)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=list(TOPICS), default="separate")
    parser.add_argument("--payment-reader", choices=["v1", "v2"], default="v2")
    parser.add_argument("--checkpoint", help="New directory to replay; otherwise resume the mode/reader checkpoint")
    parser.add_argument("--sqlite-path", default="data/events.db", help="SQLite output file (default: data/events.db)")
    triggers = parser.add_mutually_exclusive_group()
    triggers.add_argument("--available-now", action="store_true", help="Drain available records in micro-batches and exit")
    triggers.add_argument("--once", action="store_true", help="One micro-batch then exit (legacy Spark trigger)")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=1000,
                        help="Total Kafka offset budget per micro-batch; ignored by --once")
    parser.add_argument("--task-delay-seconds", type=float, default=0,
                        help="Optional pause per nonempty worker task to make parallelism visible")
    args = parser.parse_args()
    if args.max_offsets_per_trigger < 1:
        parser.error("--max-offsets-per-trigger must be positive")
    if not math.isfinite(args.task_delay_seconds) or args.task_delay_seconds < 0:
        parser.error("--task-delay-seconds must be finite and nonnegative")
    configure_logging()
    registry = Registry()
    spark = (SparkSession.builder.appName(f"billing-{args.mode}-{args.payment_reader}")
             .config("spark.sql.shuffle.partitions", "3").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    checkpoint = args.checkpoint or os.path.join(os.getenv("CHECKPOINT_ROOT", ".checkpoints"),
                                                f"{args.mode}-{args.payment_reader}")
    logging.info("START mode=%s topics=%s reader=%s checkpoint=%s", args.mode, TOPICS[args.mode],
                 args.payment_reader, checkpoint)
    try:
        source = (spark.readStream.format("kafka")
                           .option("kafka.bootstrap.servers", BOOTSTRAP)
                           .option("subscribe", TOPICS[args.mode])
                           .option("startingOffsets", "earliest")
                           .option("failOnDataLoss", "true"))
        if not args.once:
            source = source.option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        else:
            logging.warning("--once uses one batch without a maxOffsetsPerTrigger limit; prefer --available-now")
        raw = wire_columns(source.load())
        if args.mode == "envelope":
            # A genuinely structured streaming DataFrame: typed nested columns exist
            # before foreachBatch, so SQL filters/aggregations can go here.
            events = envelope_frame(raw, registry)
            events.printSchema()
        else:
            raw.printSchema()
            events = raw

        def process_batch(batch, batch_id):
            started = time.monotonic()
            logging.info("BATCH_START batch_id=%s mode=%s", batch_id, args.mode)
            save_batch(batch, batch_id, registry, args.payment_reader, args.mode, args.sqlite_path,
                       args.task_delay_seconds)
            logging.info("BATCH_END batch_id=%s elapsed_seconds=%.3f", batch_id, time.monotonic() - started)

        sink = events.writeStream.foreachBatch(process_batch)
        sink = sink.option("checkpointLocation", checkpoint)
        if args.available_now:
            sink = sink.trigger(availableNow=True)
        elif args.once:
            sink = sink.trigger(once=True)
        else:
            sink = sink.trigger(processingTime="5 seconds")
        sink.start().awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
