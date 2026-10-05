"""Fast tests: real Avro bytes and writer/reader resolution, no services needed."""

import struct
import unittest
from unittest.mock import Mock

from common import Registry, decode, encode, schema, split_wire
from producer import envelope, make_events


class WireTests(unittest.TestCase):
    def test_header_and_registry_schema_are_separate(self):
        event = make_events("v1")[0][2]
        value = encode(event, schema("payment_v1"), 1234)
        self.assertEqual(value[:5], b"\x00" + struct.pack(">I", 1234))
        self.assertEqual(split_wire(value)[0], 1234)
        self.assertNotIn(b"PaymentCreated", value)
        self.assertNotIn(b"user_id", value)
        self.assertEqual(decode(value, schema("payment_v1")), event)

    def test_all_payment_writer_reader_combinations(self):
        for writer_version in ("v1", "v2"):
            for reader_version in ("v1", "v2"):
                with self.subTest(writer=writer_version, reader=reader_version):
                    event = make_events(writer_version)[0][2]
                    writer, reader = schema(f"payment_{writer_version}"), schema(f"payment_{reader_version}")
                    result = decode(encode(event, writer, 1), writer, reader)
                    self.assertEqual(result["event_id"], event["event_id"])
                    if reader_version == "v1":
                        self.assertNotIn("payment_method", result)
                    else:
                        self.assertEqual(result["payment_method"], "card" if writer_version == "v2" else None)

    def test_refund_and_envelopes(self):
        for event_type, name, event in make_events("v2"):
            self.assertEqual(decode(encode(event, schema(name), 8), schema(name)), event)
            wrapped = envelope(event_type, event)
            self.assertEqual(decode(encode(wrapped, schema("envelope"), 9), schema("envelope")), wrapped)

    def test_invalid_headers(self):
        for value in (None, b"", b"\x00\x01", b"\x01\x00\x00\x00\x01"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                split_wire(value)

    def test_registry_caches_by_id(self):
        registry = Registry()
        registry.request = Mock(return_value={"schema": '{"type":"string"}'})
        self.assertEqual(registry.get(123), {"type": "string"})
        registry.get(123)
        registry.request.assert_called_once_with("GET", "/schemas/ids/123")


if __name__ == "__main__":
    unittest.main()
