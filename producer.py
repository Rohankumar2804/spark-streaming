"""Produce real Avro bytes; log the five-byte header and broker delivery metadata."""

import argparse
import json
import logging
import time
import uuid

from confluent_kafka import Producer

from common import BOOTSTRAP, TOPIC_PARTITIONS, Registry, configure_logging, encode, fullname, schema, split_wire


def make_events(version):
    payment = {"event_id": str(uuid.uuid4()), "user_id": "user-42", "amount": 49.95, "currency": "USD"}
    if version == "v2":
        payment["payment_method"] = "card"
    refund = {"event_id": str(uuid.uuid4()), "payment_id": payment["event_id"],
              "refund_amount": 9.95, "reason": "partial return"}
    return [("PAYMENT_CREATED", f"payment_{version}", payment), ("REFUND_CREATED", "refund", refund)]


def envelope(event_type, event):
    # Exactly one typed payload slot is present; event_id lives in the outer record.
    payload = {k: v for k, v in event.items() if k != "event_id"}
    return {"event_id": event["event_id"], "event_type": event_type, "timestamp": time.time_ns() // 1_000_000,
            "payload": {"payment": payload if event_type == "PAYMENT_CREATED" else None,
                        "refund": payload if event_type == "REFUND_CREATED" else None}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["separate", "shared", "envelope"], default="separate")
    parser.add_argument("--payment-version", choices=["v1", "v2", "both"], default="both")
    parser.add_argument("--count", type=int, default=1, help="Number of payment/refund pairs per version")
    parser.add_argument("--round-robin", action="store_true",
                        help="Send successive pairs explicitly to partitions 0, 1, 2 for the parallelism demo")
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be positive")
    configure_logging()
    registry = Registry()
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True,
                         "acks": "all", "delivery.timeout.ms": 30000})
    ids, errors = {}, []
    versions = ["v1", "v2"] if args.payment_version == "both" else [args.payment_version]
    pair_number = 0
    for version in versions:
        for _ in range(args.count):
            # Explicit assignment guarantees all three partitions get data with count >= 3.
            partition_options = {"partition": pair_number % TOPIC_PARTITIONS} if args.round_robin else {}
            pair_number += 1
            for event_type, schema_file, event in make_events(version):
                topic = "payments" if event_type == "PAYMENT_CREATED" else "refunds"
                if args.mode == "shared":
                    topic = "billing-events"
                if args.mode == "envelope":
                    topic, schema_file, event = "billing-envelopes", "envelope", envelope(event_type, event)
                writer = schema(schema_file)
                subject = f"{topic}-{fullname(writer)}" if args.mode == "shared" else f"{topic}-value"
                cache_key = (subject, schema_file)
                if cache_key not in ids:
                    ids[cache_key] = registry.lookup(subject, writer)
                schema_id = ids[cache_key]
                value = encode(event, writer, schema_id)
                _, payload = split_wire(value)
                logging.info("WIRE event_type=%s topic=%s magic=0 schema_id=%s header_hex=%s "
                             "avro_payload_hex=%s total_bytes=%s schema_json_bytes=%s",
                             event_type, topic, schema_id, value[:5].hex(), payload.hex(), len(value),
                             len(json.dumps(writer).encode()))

                def delivered(error, message, event_type=event_type, schema_id=schema_id, event=event):
                    if error:
                        errors.append(str(error))
                        logging.error("DELIVERY_FAILED %s", error)
                    else:
                        logging.info("DELIVERED event_type=%s schema_id=%s topic=%s partition=%s offset=%s event=%s",
                                     event_type, schema_id, message.topic(), message.partition(), message.offset(),
                                     json.dumps(event))

                # Link payment/refund keys so each pair shares a partition in the shared topic.
                key = event["event_id"] if event_type == "PAYMENT_CREATED" else (
                    event["payload"]["refund"]["payment_id"] if args.mode == "envelope" else event["payment_id"])
                producer.produce(topic, key=key.encode(), value=value, on_delivery=delivered, **partition_options)
                producer.poll(0)
    remaining = producer.flush(35)
    if remaining or errors:
        raise RuntimeError(f"Delivery failed: outstanding={remaining}, errors={errors}")


if __name__ == "__main__":
    main()
