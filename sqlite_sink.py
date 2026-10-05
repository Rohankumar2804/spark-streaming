"""One driver-side SQLite transaction per micro-batch, with replay deduplication."""

from contextlib import contextmanager
from pathlib import Path
import sqlite3


@contextmanager
def batch_transaction(database):
    path = Path(database).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30)
    try:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS events (
                topic TEXT NOT NULL,
                kafka_partition INTEGER NOT NULL,
                kafka_offset INTEGER NOT NULL,
                schema_id INTEGER NOT NULL,
                event_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_json TEXT NOT NULL CHECK(json_valid(event_json)),
                batch_id INTEGER NOT NULL,
                PRIMARY KEY (topic, kafka_partition, kafka_offset)
            )
        """)
        # Explicit BEGIN includes all schema groups in one transaction. Exceptions
        # roll back the whole batch; foreachBatch returns only after commit succeeds.
        with connection:
            connection.execute("BEGIN")
            yield connection
    finally:
        connection.close()


def insert_events(connection, rows):
    before = connection.total_changes
    connection.executemany("""
        INSERT INTO events
            (topic, kafka_partition, kafka_offset, schema_id, event_id, event_type, event_json, batch_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(topic, kafka_partition, kafka_offset) DO NOTHING
    """, rows)
    # Only duplicate Kafka coordinates are ignored; invalid JSON/nulls still fail.
    return connection.total_changes - before
