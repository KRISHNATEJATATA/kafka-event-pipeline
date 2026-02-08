import json
from pathlib import Path
from jsonschema import Draft202012Validator

# Use schemas from final_consumer directory
PAYLOAD_SCHEMA_DIR = Path(__file__).parent / "final_consumer" / "schemas" / "payload"


def load_payload_validators():
    validators = {}

    for schema_file in PAYLOAD_SCHEMA_DIR.glob("*.json"):
        event_type = schema_file.stem  # filename == event_type
        schema = json.loads(schema_file.read_text())
        validators[event_type] = Draft202012Validator(schema)

    return validators


PAYLOAD_VALIDATORS = load_payload_validators()
