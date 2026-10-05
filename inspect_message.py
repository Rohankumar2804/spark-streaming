"""Read actual Kafka records, print framing, fetch their writer schema and decode."""

import argparse
import json
import logging
import time
import uuid

from confluent_kafka import Consumer

from common import BOOTSTRAP, Registry, configure_logging, decode, fullname, split_wire


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", default="billing-events")
    parser.add_argument("--count", type=int, default=4)
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be positive")
    configure_logging()
    registry = Registry()
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"inspect-{uuid.uuid4()}",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([args.topic])
    seen, deadline = 0, time.monotonic() + 45
    try:
        while seen < args.count and time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(message.error())
            value = message.value()
            schema_id, payload = split_wire(value)
            writer = registry.get(schema_id)
            event = decode(value, writer)
            logging.info("INSPECT topic=%s partition=%s offset=%s magic=%s schema_id=%s "
                         "schema_id_hex=%s payload_hex=%s event_type=%s decoded_event=%s",
                         message.topic(), message.partition(), message.offset(), value[0], schema_id,
                         value[1:5].hex(), payload.hex(), event.get("event_type", fullname(writer)), json.dumps(event))
            seen += 1
        if seen < args.count:
            raise TimeoutError(f"Read {seen}/{args.count} messages; produce messages or reduce --count")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
