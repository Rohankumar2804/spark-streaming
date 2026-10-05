"""Exercise actual Spark Avro expressions without needing Kafka or Registry."""

import tempfile
import json
import sqlite3
from functools import partial
from unittest.mock import patch

from pyspark.sql import SparkSession, functions as F

from common import encode, schema
from producer import envelope, make_events
from spark_consumer import decode_frame, envelope_frame, save_batch, log_partition, log_rows, wire_columns


class StubRegistry:
    def lookup(self, subject, writer):
        return 20

    def get(self, schema_id):
        return schema("envelope")


def main():
    spark = SparkSession.builder.master("local[3]").appName("avro-checks").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    def frame(value):
        return wire_columns(spark.createDataFrame([(value, "test", 0, 42)],
                            "value binary, topic string, partition int, offset long"))

    try:
        for writer_version in ("v1", "v2"):
            writer = schema(f"payment_{writer_version}")
            event = make_events(writer_version)[0][2]
            raw = frame(encode(event, writer, 1000))
            assert raw.first().schema_id == 1000
            for reader_version in ("v1", "v2"):
                result = decode_frame(raw, writer, schema(f"payment_{reader_version}")).first().event.asDict()
                assert result["event_id"] == event["event_id"]
                if reader_version == "v1":
                    assert "payment_method" not in result
                else:
                    assert result["payment_method"] == ("card" if writer_version == "v2" else None)
        for event_type, name, event in make_events("v2"):
            result = decode_frame(frame(encode(event, schema(name), 5)), schema(name), schema(name)).first()
            assert result.event.asDict() == event
            wrapped = envelope(event_type, event)
            result = envelope_frame(frame(encode(wrapped, schema("envelope"), 20)), StubRegistry()).first()
            assert result.event_type == event_type
            assert result.event_id == event["event_id"]
        log_rows(envelope_frame(frame(encode(wrapped, schema("envelope"), 20)), StubRegistry()), 0)

        # Real worker tasks: three nonempty partitions must overlap in time.
        from pyspark.sql import Row
        parallel_rows = [Row(topic="parallel-check", partition=i, json='{"event_type":"TEST"}') for i in range(3)]
        summaries = spark.sparkContext.parallelize(parallel_rows, 3).mapPartitions(
            partial(log_partition, batch_id=99, task_delay_seconds=2)).collect()
        assert {task["spark_partition_id"] for task in summaries} == {0, 1, 2}
        assert all(task["rows"] == 1 for task in summaries)
        assert len({task["worker_pid"] for task in summaries}) == 3
        assert max(task["started_at"] for task in summaries) < min(task["ended_at"] for task in summaries)

        # Exercise a real streaming query and mixed-ID batch routing using files as
        # the source. Broker integration remains a separate Docker smoke test.
        rows, schemas_by_id = [], {}
        for version in ("v1", "v2"):
            for event_type, name, event in make_events(version):
                schema_id = {"payment_v1": 1, "payment_v2": 2, "refund": 3}[name]
                schemas_by_id[schema_id] = schema(name)
                rows.append((encode(event, schema(name), schema_id), "billing-events", 0, len(rows)))
        source = spark.createDataFrame(rows, "value binary, topic string, partition int, offset long")
        decoded_rows = []

        def collect_rows(df, batch_id, task_delay_seconds=0):
            decoded_rows.extend(df.collect())

        class MixedRegistry:
            def get(self, schema_id):
                return schemas_by_id[schema_id]

        with tempfile.TemporaryDirectory(prefix="billing-spark-") as directory:
            source.write.parquet(directory + "/input")
            stream = wire_columns(spark.readStream.schema(source.schema).parquet(directory + "/input"))
            with patch("spark_consumer.log_rows", collect_rows):
                query = (stream.writeStream.foreachBatch(lambda df, batch_id:
                         save_batch(df, batch_id, MixedRegistry(), "v2", "shared", directory + "/events.db"))
                         .option("checkpointLocation", directory + "/checkpoint")
                         .trigger(availableNow=True).start())
                query.awaitTermination()
            # Replay the same Kafka coordinates with another batch ID: no duplicates.
            with patch("spark_consumer.log_rows"):
                save_batch(wire_columns(source), 1, MixedRegistry(), "v2", "shared", directory + "/events.db")
                separate = wire_columns(source).withColumn("topic", F.when(F.col("schema_id") == 3,
                                                                            "refunds").otherwise("payments"))
                save_batch(separate, 0, MixedRegistry(), "v2", "separate", directory + "/separate.db")
                envelope_rows = [(encode(envelope(kind, event), schema("envelope"), 20), "billing-envelopes", 0, i)
                                 for i, (kind, name, event) in enumerate(make_events("v2"))]
                envelopes = envelope_frame(wire_columns(spark.createDataFrame(envelope_rows, source.schema)), StubRegistry())
                save_batch(envelopes, 0, StubRegistry(), "v2", "envelope", directory + "/envelopes.db")
            for filename in ("events.db", "separate.db"):
                with sqlite3.connect(directory + "/" + filename) as connection:
                    saved = connection.execute("SELECT event_type, event_json FROM events").fetchall()
                    assert len(saved) == 4
                    methods = {json.loads(body)["payment_method"] for kind, body in saved if kind == "PAYMENT_CREATED"}
                    assert methods == {None, "card"}
            with sqlite3.connect(directory + "/envelopes.db") as connection:
                saved = [json.loads(row[0]) for row in connection.execute("SELECT event_json FROM events")]
                assert len(saved) == 2
                assert {event["event_type"] for event in saved} == {"PAYMENT_CREATED", "REFUND_CREATED"}
                assert all("payload" in event and "timestamp" in event for event in saved)
        assert len(decoded_rows) == 4
        assert {row.event_type for row in decoded_rows} == {"PAYMENT_CREATED", "REFUND_CREATED"}
        payments = [row.event.asDict() for row in decoded_rows if row.event_type == "PAYMENT_CREATED"]
        assert {row["payment_method"] for row in payments} == {None, "card"}
        # Invalid header and mismatched envelope contract must fail, not vanish.
        bad = envelope("PAYMENT_CREATED", make_events("v1")[0][2])
        bad["event_type"] = "REFUND_CREATED"
        for df, expected in [(frame(b"\x01\x00\x00\x00\x01").select("schema_id"), "Invalid Confluent"),
                   (envelope_frame(frame(encode(bad, schema("envelope"), 20)), StubRegistry()), "event_type must match"),
                   (envelope_frame(frame(encode(envelope("PAYMENT_CREATED", make_events("v1")[0][2]),
                                               schema("envelope"), 21)), StubRegistry()), "Unknown envelope")]:
            try:
                df.collect()
            except Exception as exc:
                assert expected in str(exc), str(exc)
            else:
                raise AssertionError("Malformed/unsupported record was accepted")
        print("PASS: Avro evolution, streaming routing, parallel tasks, SQLite all modes/replay, rejection checks")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
