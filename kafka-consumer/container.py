import os
import logging
from confluent_kafka.admin import AdminClient, NewTopic
from consumer.consumer import KafkaConsumerApp as KafkaConsumerRunner

logger = logging.getLogger(__name__)


class Container:
    """
    Dependency Injection Container

    Wires:
    - Kafka consumer
    - Domain processing
    - Infrastructure health checks
    """

    def __init__(self):
        # Kafka configuration
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.consumer_group = os.getenv(
            "KAFKA_CONSUMER_GROUP",
            "recommender-event-processor",
        )

        # Lazily initialized components
        self._consumer: KafkaConsumerRunner | None = None
        self._admin_client: AdminClient | None = None

        logger.info("Container initialized with configuration:")
        logger.info(f"  Kafka Bootstrap Servers: {self.bootstrap_servers}")
        logger.info(f"  Kafka Consumer Group: {self.consumer_group}")

    # =========================
    # Kafka Consumer
    # =========================
    async def get_consumer(self) -> KafkaConsumerRunner:
        """
        Get Kafka consumer runner (lazy init, singleton).
        
        IMPORTANT: This returns a cached singleton instance.
        Multiple calls return the SAME consumer instance.
        
        The consumer lifecycle is managed by WorkerApp.shutdown().
        Container.close() delegates to consumer.disconnect() but does NOT
        create double-shutdown scenarios because disconnect() is idempotent.
        
        Returns:
            Singleton KafkaConsumerRunner instance
        """
        if self._consumer is None:
            self._consumer = KafkaConsumerRunner()
            logger.info("KafkaConsumerRunner created (singleton)")

        return self._consumer

    # =========================
    # Kafka Admin (health)
    # =========================
    def _get_admin_client(self) -> AdminClient:
        if self._admin_client is None:
            self._admin_client = AdminClient(
                {"bootstrap.servers": self.bootstrap_servers}
            )
        return self._admin_client

    async def create_topics(self):
        """
        Create required Kafka topics if they don't exist
        """
        admin = self._get_admin_client()
        topics = ["kafka-topics1", "kafka-topics2"]
        existing_topics = admin.list_topics(timeout=10).topics
        topics_to_create = [topic for topic in topics if topic not in existing_topics]
        if topics_to_create:
            new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics_to_create]
            futures = admin.create_topics(new_topics)
            for topic, future in futures.items():
                try:
                    future.result()  # Wait for topic creation
                    logger.info(f"Created topic: {topic}")
                except Exception as e:
                    logger.warning(f"Failed to create topic {topic}: {e}")

    # =========================
    # Health Check
    # =========================
    async def health_check(self) -> dict:
        """
        Check health of Kafka and internal components
        """
        health_status = {
            "status": "healthy",
            "components": {},
        }

        try:
            # --- Kafka broker connectivity ---
            admin = self._get_admin_client()
            cluster_md = admin.list_topics(timeout=5)

            if cluster_md.brokers:
                health_status["components"]["kafka"] = "healthy"
            else:
                health_status["components"]["kafka"] = "unhealthy"

            # --- Consumer state ---
            if self._consumer is None:
                health_status["components"]["consumer"] = "not_started"
            else:
                health_status["components"]["consumer"] = "running"

            # --- Overall ---
            if any(
                status == "unhealthy"
                for status in health_status["components"].values()
            ):
                health_status["status"] = "unhealthy"

        except Exception as e:
            logger.error(f"Health check failed: {e}", exc_info=True)
            health_status["status"] = "unhealthy"
            health_status["error"] = str(e)

        return health_status

    # =========================
    # Shutdown / Cleanup
    # =========================
    async def close(self):
        """
        Gracefully close all managed resources.
        
        This method is IDEMPOTENT - safe to call multiple times.
        It delegates to consumer.disconnect() which is also idempotent.
        
        NOTE: WorkerApp.shutdown() is the primary shutdown orchestrator.
        This method is called by WorkerApp after consumer.disconnect(),
        but won't cause double-shutdown because of the _consumer check.
        """
        logger.info("Container shutdown initiated")

        try:
            # Check if consumer exists and hasn't been cleaned up yet
            if self._consumer:
                # Only disconnect if WorkerApp hasn't already done it
                # This is safe because disconnect() is idempotent
                try:
                    await self._consumer.disconnect()
                except Exception as e:
                    logger.error(f"Error disconnecting consumer: {e}", exc_info=True)

            # AdminClient does not require explicit close,
            # but we drop references to avoid reuse.
            self._admin_client = None
            self._consumer = None

            logger.info("Container cleanup completed")

        except Exception as e:
            logger.error(f"Error during container cleanup: {e}", exc_info=True)


# Global container instance (matches template pattern)
container = Container()
