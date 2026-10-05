"""Create topics and register ordered schema versions; safe to rerun."""

import logging
from urllib.parse import quote

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic
from confluent_kafka.error import KafkaError

from common import BOOTSTRAP, TOPIC_PARTITIONS, Registry, configure_logging, schema


def main():
    configure_logging()
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    topics = [NewTopic(t, num_partitions=TOPIC_PARTITIONS, replication_factor=1)
              for t in ("payments", "refunds", "billing-events", "billing-envelopes")]
    for topic, future in admin.create_topics(topics, request_timeout=30).items():
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
        metadata = admin.list_topics(topic=topic, timeout=10).topics[topic]
        if metadata.error:
            raise KafkaException(metadata.error)
        count = len(metadata.partitions)
        # Upgrade the earlier two-partition demo without deleting messages.
        if count < TOPIC_PARTITIONS:
            admin.create_partitions([NewPartitions(topic, TOPIC_PARTITIONS)], request_timeout=30)[topic].result()
            logging.info("PARTITIONS_INCREASED topic=%s old=%s new=%s", topic, count, TOPIC_PARTITIONS)
        elif count > TOPIC_PARTITIONS:
            raise ValueError(f"{topic} already has {count} partitions; Kafka cannot shrink to {TOPIC_PARTITIONS}")
        logging.info("TOPIC_READY topic=%s partitions=%s", topic, TOPIC_PARTITIONS)

    registry = Registry()
    subjects = {
        # TopicNameStrategy: a subject for each topic's value.
        "payments-value": ["payment_v1", "payment_v2"],
        "refunds-value": ["refund"],
        # TopicRecordNameStrategy: each record type evolves independently in this topic.
        "billing-events-demo.billing.PaymentCreated": ["payment_v1", "payment_v2"],
        "billing-events-demo.billing.RefundCreated": ["refund"],
        "billing-envelopes-value": ["envelope"],
    }
    for subject, versions in subjects.items():
        registry.request("PUT", f"/config/{quote(subject, safe='')}",
                         json={"compatibility": "BACKWARD_TRANSITIVE"})
        for version in versions:
            schema_id = registry.register(subject, schema(version))
            logging.info("REGISTERED subject=%s file=%s schema_id=%s", subject, version, schema_id)


if __name__ == "__main__":
    main()
