"""Prove a breaking schema is rejected; successful schemas remain unchanged."""

import copy
import json

from common import Registry, schema


def main():
    registry = Registry()
    incompatible = copy.deepcopy(schema("payment_v2"))
    incompatible["fields"].append({"name": "required_country", "type": "string"})
    body = {"schemaType": "AVRO", "schema": json.dumps(incompatible)}
    before = registry.request("GET", "/subjects/payments-value/versions")
    result = registry.request("POST", "/compatibility/subjects/payments-value/versions/latest", json=body)
    assert result["is_compatible"] is False, result
    response = registry.session.post(registry.url + "/subjects/payments-value/versions", json=body, timeout=10)
    assert response.status_code == 409, (response.status_code, response.text)
    assert registry.request("GET", "/subjects/payments-value/versions") == before
    print("PASS: required field without a default is incompatible; registration rejected with HTTP 409")


if __name__ == "__main__":
    main()
