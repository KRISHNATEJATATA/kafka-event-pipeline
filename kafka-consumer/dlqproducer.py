from confluent_kafka import Producer
import json
import os
producer_conf = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "linger.ms": 5,
}

dlq_producer = Producer(producer_conf)
def send_to_dlq(msg):
    # msg is now a payload dict
    topic = msg.get("topic")
    key = msg.get("key")
    dlq_topic = DLQ_TOPIC_MAP.get(topic)
    if not dlq_topic:
        raise RuntimeError(f"No DLQ configured for topic {topic}")

    dlq_producer.produce(
        topic=dlq_topic,
        key=key.encode("utf-8") if key else None,
        value=json.dumps(msg).encode("utf-8"),
    )
    dlq_producer.flush()
DLQ_TOPIC_MAP = {
    "kafka-topics1": "dlq-topics1",
    "kafka-topics2": "dlq-topics2",
}