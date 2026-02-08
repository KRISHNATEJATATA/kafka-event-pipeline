# Kafka Event-Driven Processing System

[![Docker Compose](https://img.shields.io/badge/Docker-Compose-blue?style=flat-square&logo=docker)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.12+-3776ab?style=flat-square&logo=python&logoColor=white)](https://www.python.org)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat-square&logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)
[![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=flat-square&logo=prometheus&logoColor=white)](https://prometheus.io/)

A production-ready event-driven system built with Apache Kafka, featuring robust message processing with schema validation, retry policies, dead-letter queues, and real-time metrics monitoring.

[Overview](#overview) • [Features](#features) • [Getting Started](#getting-started) • [Architecture](#architecture) • [Configuration](#configuration) • [Monitoring](#monitoring)

---

## Overview

This project demonstrates a complete Kafka-based event processing pipeline designed for reliability and observability. It handles event ingestion, validation, processing, and monitoring with enterprise-grade patterns including automatic retries, dead-letter queue handling, and comprehensive metrics.

The system processes events for a sample domain (file management and user operations) but can be easily adapted to any event-driven use case.

> This project is ready to run locally using Docker Compose with no external dependencies required.

## Features

- **Schema-Based Validation** - JSON Schema validation for both envelope and payload structures
- **Automatic Retry Logic** - Configurable retry policies with exponential backoff
- **Dead Letter Queue (DLQ)** - Failed messages automatically routed to DLQ for analysis
- **Real-time Metrics** - Prometheus metrics for consumer lag, processing rates, and health monitoring
- **Correlation Tracking** - End-to-end request tracing with correlation IDs
- **Offset Management** - Reliable offset tracking and batch commits for optimal performance
- **Async Processing** - Non-blocking message processing with asyncio
- **Health Checks** - Built-in health check endpoints for container orchestration
- **Multiple Event Types** - Supports file-added, file-removed, user-created, and user-deleted events

## Architecture

The system consists of three main components:

```
┌──────────────┐         ┌──────────────┐         ┌──────────────────┐
│              │         │              │         │                  │
│   Producer   │────────▶│    Kafka     │────────▶│     Consumer     │
│              │         │   Broker     │         │   + Validator    │
└──────────────┘         └──────────────┘         └──────────────────┘
                                │                          │
                                │                          │
                                ▼                          ▼
                         ┌──────────────┐         ┌──────────────────┐
                         │              │         │                  │
                         │  Zookeeper   │         │   Prometheus     │
                         │              │         │    Metrics       │
                         └──────────────┘         └──────────────────┘
```

### Components

**Producer**
- Generates sample events (file operations and user management)
- Validates payloads against JSON schemas before publishing
- Uses dual Kafka listeners for Docker and host connectivity

**Consumer**
- Thread-based polling to prevent blocking the async event loop
- Multi-stage validation (envelope → event type → payload)
- Configurable batch processing and commit intervals
- Automatic retry with configurable backoff
- DLQ routing for permanently failed messages

**Infrastructure**
- **Kafka**: Message broker on ports 9092 (host) and 9093 (container)
- **Zookeeper**: Kafka cluster coordination
- **Prometheus**: Metrics collection endpoint on port 8000

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/get-started/) and [Docker Compose](https://docs.docker.com/compose/install/)
- Python 3.12+ (optional, for local development)

### Quick Start

1. **Clone and navigate to the project:**
   ```bash
   git clone https://github.com/KRISHNATEJATATA/kafka-event-pipeline.git
   cd kafka
   ```

2. **Start the entire stack:**
   ```bash
   docker-compose up --build
   ```

3. **Verify the system is running:**
   
   The consumer exposes metrics at `http://localhost:8000/metrics`
   
   Check consumer logs:
   ```bash
   docker logs -f kafka-consumer
   ```
   
   Check producer logs:
   ```bash
   docker logs -f producer
   ```

4. **Stop the system:**
   ```bash
   docker-compose down
   ```

### Running Locally (Without Docker)

If you want to develop locally:

1. **Start Kafka infrastructure:**
   ```bash
   docker-compose up zookeeper kafka
   ```

2. **Install Python dependencies:**
   ```bash
   # Consumer
   cd kafka-consumer
   pip install -r requirements.txt
   
   # Producer
   cd ../producer
   pip install -r requirements.txt
   ```

3. **Run the consumer:**
   ```bash
   cd kafka-consumer
   export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   python main.py
   ```

4. **Run the producer (in another terminal):**
   ```bash
   cd producer
   export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   python producer.py
   ```

## Configuration

### Environment Variables

**Consumer Configuration:**

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `CONSUMER_GROUP_ID` | `kafka-group` | Consumer group identifier |
| `KAFKA_TOPICS` | `kafka-topics1,kafka-topics2` | Comma-separated topic list |
| `MAX_BATCH_SIZE` | `100` | Maximum messages per batch |
| `POLL_TIMEOUT_SEC` | `1.0` | Kafka poll timeout |
| `MAX_RETRIES` | `3` | Maximum retry attempts |
| `RETRY_BACKOFF_SEC` | `2` | Retry backoff duration |
| `COMMIT_INTERVAL_MESSAGES` | `100` | Messages between commits |
| `LAG_MONITOR_INTERVAL_SEC` | `60` | Lag monitoring interval |
| `METRICS_PORT` | `8000` | Prometheus metrics port |
| `LOG_LEVEL` | `INFO` | Logging level |

**Producer Configuration:**

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9093` | Kafka broker addresses |

### Event Schemas

Events follow an envelope pattern with typed payloads:

**Envelope Structure:**
```json
{
  "eventId": "unique-id",
  "eventType": "file-added|file-removed|user-created|user-deleted",
  "eventVersion": "1.0",
  "eventTime": "2024-01-01T00:00:00Z",
  "payload": { ... }
}
```

**Supported Event Types:**
- `file-added` - File addition events
- `file-removed` - File deletion events
- `user-created` - User creation events
- `user-deleted` - User deletion events

Payload schemas are defined in `kafka-consumer/schemas/payload/`.

## Monitoring

### Prometheus Metrics

The consumer exposes Prometheus metrics at `http://localhost:8000/metrics`:

- `kafka_consumer_lag` - Consumer lag per topic/partition
- Additional metrics for processing rates and errors

### Health Checks

The consumer performs periodic health checks logging:
- Consumer connectivity status
- Topic subscription status
- Processing thread health

### Dead Letter Queue

Failed messages are automatically sent to the DLQ topic for later analysis. Check DLQ messages:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9093 \
  --topic DLQ \
  --from-beginning
```

## Project Structure

```
kafka/
├── docker-compose.yml           # Docker orchestration
├── producer/
│   ├── producer.py             # Event producer implementation
│   ├── payload_schema_loader.py # Schema validation loader
│   ├── requirements.txt
│   └── Dockerfile
├── kafka-consumer/
│   ├── main.py                 # Consumer application entry
│   ├── container.py            # Dependency injection container
│   ├── dlqproducer.py         # DLQ message routing
│   ├── consumer/
│   │   ├── consumer.py        # Main consumer logic
│   │   ├── handler.py         # Message processing handlers
│   │   ├── retry_policy.py   # Retry mechanism
│   │   ├── dlq.py            # DLQ routing logic
│   │   ├── metrics.py        # Prometheus metrics
│   │   ├── offset_manager.py # Offset tracking
│   │   └── kafka_thread.py   # Non-blocking Kafka polling
│   ├── validator/
│   │   ├── core.py           # Validation logic
│   │   ├── schema_loader.py  # Schema management
│   │   └── exceptions.py     # Custom exceptions
│   ├── schemas/
│   │   ├── envelope/         # Envelope schemas
│   │   └── payload/          # Event payload schemas
│   ├── requirements.txt
│   └── Dockerfile
└── README.md
```

## Development

### Adding New Event Types

1. **Define the payload schema** in `kafka-consumer/schemas/payload/<event-type>.json`

2. **Register the event type** in `kafka-consumer/validator/constants.py`:
   ```python
   ALL_EVENT_TYPES = {
       "file-added",
       "file-removed",
       "user-created",
       "user-deleted",
       "your-new-event"  # Add here
   }
   ```

3. **Update the producer** in `producer/producer.py` to generate the new event type

4. **Implement event handling** in `kafka-consumer/consumer/handler.py`

### Running Tests

```bash
cd kafka-consumer
pytest
```

## Troubleshooting

**Consumer not connecting to Kafka:**
- Ensure Kafka is fully started: `docker logs kafka`
- Check bootstrap servers configuration matches your environment
- Verify network connectivity between containers

**Schema validation failures:**
- Check event structure matches schema in `schemas/` directory
- Verify `eventType` is registered in `validator/constants.py`
- Review validation error details in consumer logs

**High consumer lag:**
- Increase `MAX_BATCH_SIZE` for higher throughput
- Reduce `COMMIT_INTERVAL_MESSAGES` for faster commits
- Check Prometheus metrics at `http://localhost:8000/metrics`

**DLQ messages accumulating:**
- Review DLQ messages to identify patterns
- Check for schema mismatches or malformed events
- Adjust retry configuration if needed

## Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Kafka Python](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [JSON Schema](https://json-schema.org/)
- [Prometheus Python Client](https://github.com/prometheus/client_python)
- [Event-Driven Architecture Patterns](https://www.enterpriseintegrationpatterns.com/patterns/messaging/)

## License

This project is available under the MIT License.
