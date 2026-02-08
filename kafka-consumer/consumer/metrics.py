from prometheus_client import Gauge, start_http_server

# Gauge for consumer lag per topic/partition
gauge_consumer_lag = Gauge(
    'kafka_consumer_lag',
    'Kafka consumer lag (latest offset - committed offset)',
    ['topic', 'partition']
)

def set_consumer_lag(topic: str, partition: int, lag: int):
    gauge_consumer_lag.labels(topic=topic, partition=partition).set(lag)

# Start Prometheus metrics server (default port 8000)
def start_metrics_server(port: int = 8000):
    start_http_server(port)
