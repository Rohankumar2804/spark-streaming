#!/bin/sh
set -eu
docker compose up -d --wait kafka schema-registry
docker compose build app
docker compose run --rm app python setup_demo.py
docker compose run --rm app python check_compatibility.py
docker compose run --rm app python -m unittest discover -s tests -p 'test_*.py' -v
run_id="$(date +%s)-$$"
for mode in separate shared envelope; do
  docker compose run --rm app python producer.py --mode "$mode" --payment-version both --count 3 --round-robin
  docker compose run --rm app sh scripts/spark.sh --mode "$mode" --available-now --max-offsets-per-trigger 6 --checkpoint "/checkpoints/smoke-$run_id-$mode"
done
docker compose run --rm app python inspect_message.py --topic billing-events --count 4
docker compose run --rm app sh scripts/spark.sh --mode separate --payment-reader v1 --available-now --checkpoint "/checkpoints/smoke-$run_id-old"
docker compose run --rm app sh scripts/spark.sh --mode envelope --once --checkpoint "/checkpoints/smoke-$run_id-once"
docker compose run --rm --no-deps app python inspect_sqlite.py --limit 4
