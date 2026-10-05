"""Print saved events as readable JSON without needing the sqlite3 command-line tool."""

import argparse
import json
from pathlib import Path
import sqlite3


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sqlite-path", default="data/events.db")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("--limit must be positive")
    path = Path(args.sqlite_path).expanduser().resolve()
    if not path.is_file():
        parser.error(f"Database does not exist: {path}. Run the Spark consumer first.")
    # Read-only mode prevents inspection from accidentally creating or modifying a DB.
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        count = connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        print(f"database={path} total_events={count}")
        for row in connection.execute("SELECT * FROM events ORDER BY topic, kafka_partition, kafka_offset LIMIT ?",
                                      (args.limit,)):
            event = dict(row)
            event["event_json"] = json.loads(event["event_json"])
            print(json.dumps(event, indent=2))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
