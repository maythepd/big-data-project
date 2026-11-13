"""Message contract: JSON Schemas (config/schemas) registered in a Confluent Schema Registry.

The registry keeps every version of a topic's schema and refuses a new one that breaks its
compatibility rule (BACKWARD here: the new schema must read data written with the old one).
A producer that goes through Contract checks each message before it reaches the topic:

    contract = Contract("http://schema-registry:8081", "replay-r1-value")
    schema_id = contract.register("openaq_v2")       # None + contract.errors[name] if refused
    problem = contract.check("openaq_v2", payload)    # None, or why the message breaks the contract
"""
import json
from pathlib import Path

import requests

SCHEMAS = Path(__file__).resolve().parents[2] / "config" / "schemas"
CONTENT_TYPE = {"Content-Type": "application/vnd.schemaregistry.v1+json"}


class Contract:
    def __init__(self, registry_url, subject, compatibility="BACKWARD"):
        self.url, self.subject = registry_url.rstrip("/"), subject
        self.ids, self.errors, self.validators = {}, {}, {}
        r = requests.put(f"{self.url}/config/{subject}", json={"compatibility": compatibility},
                         headers=CONTENT_TYPE, timeout=10)
        r.raise_for_status()

    def schema(self, name):
        return json.loads((SCHEMAS / f"{name}.json").read_text(encoding="utf-8"))

    def register(self, name):
        """Register a schema version; returns its id, or None when the registry refuses it."""
        if name in self.ids or name in self.errors:
            return self.ids.get(name)
        body = {"schemaType": "JSON", "schema": json.dumps(self.schema(name))}
        r = requests.post(f"{self.url}/subjects/{self.subject}/versions", json=body, headers=CONTENT_TYPE,
                          timeout=10)
        if r.ok:
            self.ids[name] = r.json()["id"]
        else:
            self.errors[name] = f"HTTP {r.status_code}: {r.json().get('message', r.text)[:300]}"
        return self.ids.get(name)

    def check(self, name, data):
        """None when `data` (bytes) is valid JSON matching a registered schema, else the reason."""
        import jsonschema
        if self.register(name) is None:
            return "incompatible_schema"
        if name not in self.validators:
            self.validators[name] = jsonschema.Draft7Validator(self.schema(name))
        try:
            msg = json.loads(data)
        except ValueError:
            return "not_json"
        error = next(iter(self.validators[name].iter_errors(msg)), None)
        if error is None:
            return None
        where = "/".join(map(str, error.absolute_path)) or error.validator
        return f"invalid:{where}"
