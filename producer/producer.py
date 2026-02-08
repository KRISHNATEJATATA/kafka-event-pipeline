
import json
import time
import random
from kafka import KafkaProducer
import uuid
import os
from dotenv import load_dotenv
from payload_schema_loader import PAYLOAD_VALIDATORS

load_dotenv()
# Use environment variable for Kafka bootstrap servers, default to 'kafka:9093' for Docker
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
TOPIC = "kafka-topics1"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

def generate_event(event_type):
    # Generate a simple event payload based on event_type
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    if event_type == "file-added":
        return {
            "fileId": str(uuid.uuid4()),
            "userId": str(uuid.uuid4()),
            "addedAt": now_iso,
        }
    elif event_type == "file-removed":
        return {
            "fileId": str(uuid.uuid4()),
            "userId": str(uuid.uuid4()),
            "removedAt": now_iso,
        }
    elif event_type == "user-created":
        return {
            "user_id": str(uuid.uuid4()),
            "username": f"user-{random.randint(1,1000)}",
            "email": f"user{random.randint(1,1000)}@example.com",
            "createdAt": now_iso,
            "roles": ["user"]
        }
    elif event_type == "user-deleted":
        return {
            "userId": str(uuid.uuid4()),
            "deletedAt": now_iso,
        }
    else:
        return {}

def main():
    event_types = list(PAYLOAD_VALIDATORS.keys())
    if not event_types:
        print("Error: No payload validators loaded. Please check your schema loader and schema files.")
        return
    while True:  # produce events continuously
        event_type = random.choice(event_types)
        payload = generate_event(event_type)
        validator = PAYLOAD_VALIDATORS[event_type]
        try:
            validator.validate(payload)
            envelope = {
                "eventId": str(uuid.uuid4()),
                "eventType": event_type,
                "eventVersion": "1.0",
                "eventTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "payload": payload
            }
            producer.send(
                TOPIC,
                key=event_type,
                value=envelope
            )
            print(f"Produced envelope: {event_type} -> {envelope}")
        except Exception as e:
            print(f"Validation failed for {event_type}: {e}")
        time.sleep(1)

if __name__ == "__main__":
    main()
