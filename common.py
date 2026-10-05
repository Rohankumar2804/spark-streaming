"""Small Schema Registry client and explicit Confluent Avro wire-format helpers."""

import io
import json
import logging
import os
from pathlib import Path
import struct
from urllib.parse import quote

from fastavro import schemaless_reader, schemaless_writer
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SCHEMAS = Path(__file__).parent / "schemas"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_PARTITIONS = 3
TOPICS = {"separate": "payments,refunds", "shared": "billing-events", "envelope": "billing-envelopes"}


def configure_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def schema(name):
    return json.loads((SCHEMAS / f"{name}.avsc").read_text())


def fullname(avro_schema):
    name = avro_schema["name"]
    return name if "." in name else f"{avro_schema['namespace']}.{name}"


class Registry:
    def __init__(self, url=None):
        self.url = (url or os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")).rstrip("/")
        self.session = requests.Session()
        # Only retry read-only GETs automatically. All requests have finite timeouts.
        self.session.mount("http://", HTTPAdapter(max_retries=Retry(
            total=3, backoff_factor=0.5, status_forcelist=[502, 503, 504], allowed_methods=["GET"]
        )))
        self.cache = {}

    def request(self, method, path, **kwargs):
        response = self.session.request(method, self.url + path, timeout=(3, 10), **kwargs)
        if not response.ok:
            raise RuntimeError(f"Registry {method} {path}: HTTP {response.status_code}: {response.text}")
        return response.json()

    def register(self, subject, avro_schema):
        return self.request("POST", f"/subjects/{quote(subject, safe='')}/versions", json={
            "schemaType": "AVRO", "schema": json.dumps(avro_schema)
        })["id"]

    def lookup(self, subject, avro_schema):
        # Exact schema lookup: never serialize v1 data using an arbitrary latest ID.
        return self.request("POST", f"/subjects/{quote(subject, safe='')}", json={
            "schemaType": "AVRO", "schema": json.dumps(avro_schema)
        })["id"]

    def get(self, schema_id):
        if schema_id not in self.cache:
            logging.info("SCHEMA_CACHE_MISS schema_id=%s", schema_id)
            result = self.request("GET", f"/schemas/ids/{schema_id}")
            if result.get("schemaType", "AVRO") != "AVRO" or result.get("references"):
                raise ValueError("This small demo supports self-contained Avro schemas only")
            self.cache[schema_id] = json.loads(result["schema"])
        return self.cache[schema_id]


def encode(event, writer_schema, schema_id):
    # schemaless_writer writes raw Avro, NOT an Avro container file with a schema header.
    payload = io.BytesIO()
    schemaless_writer(payload, writer_schema, event)
    return struct.pack(">BI", 0, schema_id) + payload.getvalue()


def split_wire(value):
    if value is None or len(value) < 5:
        raise ValueError("Expected a non-null Confluent Avro value with a five-byte header")
    magic, schema_id = struct.unpack(">BI", value[:5])
    if magic != 0:
        raise ValueError(f"Unsupported magic byte: {magic}")
    return schema_id, value[5:]


def decode(value, writer_schema, reader_schema=None):
    _, payload = split_wire(value)
    return schemaless_reader(io.BytesIO(payload), writer_schema, reader_schema)
