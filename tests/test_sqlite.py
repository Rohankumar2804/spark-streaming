import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from sqlite_sink import batch_transaction, insert_events


class SQLiteTests(unittest.TestCase):
    def test_replay_is_deduplicated_and_json_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "events.db")
            event = {"event_id": "p-1", "payment_method": None, "reason": "customer's return"}
            row = ("payments", 0, 0, 7, "p-1", "PAYMENT_CREATED", json.dumps(event), 0)
            for expected in (1, 0):
                with batch_transaction(database) as connection:
                    self.assertEqual(insert_events(connection, [row]), expected)
            with sqlite3.connect(database) as connection:
                stored = connection.execute("SELECT event_json FROM events").fetchall()
                self.assertEqual(len(stored), 1)
                self.assertEqual(json.loads(stored[0][0]), event)

    def test_failure_rolls_back_earlier_writes_in_same_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "events.db")
            valid = ("payments", 0, 0, 7, "p-1", "PAYMENT_CREATED", '{"event_id":"p-1"}', 0)
            invalid = ("refunds", 0, 0, 8, "r-1", "REFUND_CREATED", "invalid JSON", 0)
            with self.assertRaises(sqlite3.IntegrityError):
                with batch_transaction(database) as connection:
                    insert_events(connection, [valid])
                    insert_events(connection, [invalid])
            with sqlite3.connect(database) as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM events").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
