import asyncio
import logging
import os
from typing import Optional

from confluent_kafka import Consumer

from consumer.metrics import start_metrics_server, set_consumer_lag
from consumer.handler import KafkaMessageHandler, KafkaMessageProcessor
from consumer.kafka_thread import KafkaPollingThread
from consumer.offset_manager import PartitionOffsetTracker

logger = logging.getLogger(__name__)

# =====================
# Kafka configuration
# =====================

KAFKA_CONF = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "group.id": os.getenv("CONSUMER_GROUP_ID", "kafka-group"),
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
    "session.timeout.ms": int(os.getenv("SESSION_TIMEOUT_MS", "30000")),
    "heartbeat.interval.ms": int(os.getenv("HEARTBEAT_INTERVAL_MS", "10000")),
}

TOPICS = os.getenv("KAFKA_TOPICS", "kafka-topics1,kafka-topics2").split(",")

MAX_BATCH_SIZE = int(os.getenv("KAFKA_MAX_BATCH_SIZE", "100"))
POLL_TIMEOUT_SEC = float(os.getenv("KAFKA_POLL_TIMEOUT_SEC", "1.0"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF_SEC = int(os.getenv("RETRY_BACKOFF_SEC", "2"))

# Batch commit settings
COMMIT_INTERVAL_MESSAGES = int(os.getenv("COMMIT_INTERVAL_MESSAGES", "100"))

# Lag monitoring settings
LAG_MONITOR_INTERVAL_SEC = int(os.getenv("LAG_MONITOR_INTERVAL_SEC", "60"))


class KafkaConsumerApp:
    """
    Kafka consumer application.
    
    Uses thread-based I/O to prevent blocking the async event loop.
    Transport orchestration only.
    """

    def __init__(self):
        self.consumer: Optional[Consumer] = None
        self.handler = KafkaMessageHandler()
        self.processor: Optional[KafkaMessageProcessor] = None
        
        # Async queue for messages from thread
        self.message_queue: Optional[asyncio.Queue] = None
        
        # Thread for blocking Kafka I/O
        self.polling_thread: Optional[KafkaPollingThread] = None
        
        # Offset tracking
        self.offset_tracker = PartitionOffsetTracker()
        
        # State management
        self._consuming = False
        self._inflight = 0
        self._closed = False
        self._messages_since_commit = 0
        
        # Background tasks
        self._lag_monitor_task: Optional[asyncio.Task] = None
        self._commit_task: Optional[asyncio.Task] = None

        # Start Prometheus metrics server
        start_metrics_server(int(os.getenv("METRICS_PORT", "8000")))

    def _can_commit(self) -> bool:
        """Check if it's safe to commit offsets."""
        return (
            self.consumer is not None
            and not self._closed
            and bool(self.consumer.assignment())
        )

    # ---------- lifecycle ----------

    async def connect(self):
        """Initialize consumer and background tasks."""
        self.consumer = Consumer(KAFKA_CONF)
        self._closed = False
        self.consumer.subscribe(
            TOPICS,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )
        
        # Create async queue for messages
        self.message_queue = asyncio.Queue(maxsize=1000)
        
        # Create processor
        self.processor = KafkaMessageProcessor(
            consumer=self.consumer,
            message_handler=self.handler,
            offset_tracker=self.offset_tracker,
            max_retries=MAX_RETRIES,
            retry_backoff_sec=RETRY_BACKOFF_SEC,
        )
        
        logger.info("Kafka consumer connected")

    async def disconnect(self):
        """Clean shutdown of consumer and threads."""
        if self.consumer and not self._closed:
            # Stop polling thread first
            if self.polling_thread:
                self.polling_thread.stop()
                self.polling_thread.join(timeout=10.0)
                if self.polling_thread.is_alive():
                    logger.warning("Polling thread did not stop cleanly")
            
            # Wait for in-flight messages to finish
            logger.info("Waiting for in-flight messages to finish before disconnecting...")
            while self._inflight > 0:
                await asyncio.sleep(0.1)
            
            # Final offset commit
            await self._commit_offsets()
            
            # Close consumer
            self.consumer.close()
            self._closed = True
            self.processor = None
            logger.info("Kafka consumer disconnected")

    async def start(self):
        """
        Blocking start. Runs forever until stop() is called.
        
        CONTRACT: This method owns the consumer lifetime and runs forever.
        It does NOT call disconnect() - that's the caller's responsibility.
        """
        await self.connect()
        logger.info("Kafka consumer start() invoked")

        # Start background tasks
        self._lag_monitor_task = asyncio.create_task(self._monitor_lag())
        
        # Start polling thread
        self.polling_thread = KafkaPollingThread(
            consumer=self.consumer,
            message_queue=self.message_queue,
            max_batch_size=MAX_BATCH_SIZE,
            poll_timeout_sec=POLL_TIMEOUT_SEC,
        )
        self.polling_thread.start()

        # Start consuming from queue
        await self.start_consuming()

    async def stop(self):
        """
        Signal consumer to stop gracefully.
        
        CONTRACT: Only sets flags. Does NOT disconnect or close resources.
        WorkerApp owns the shutdown timing.
        """
        logger.info("Stopping Kafka consumer...")
        self._consuming = False
        
        # Cancel background tasks
        if self._lag_monitor_task:
            self._lag_monitor_task.cancel()
            try:
                await self._lag_monitor_task
            except asyncio.CancelledError:
                pass
        
        # Wait for in-flight messages
        logger.info("Waiting for in-flight messages to finish...")
        while self._inflight > 0:
            await asyncio.sleep(0.1)
        
        # Final commit
        await self._commit_offsets()
        logger.info("Kafka consumer stopped")

    # ---------- rebalance callbacks ----------

    def _on_assign(self, consumer, partitions):
        """
        Called when partitions are assigned.
        
        Runs in the Kafka polling thread - MUST be fast.
        """
        partition_strs = [f"{tp.topic}-{tp.partition}" for tp in partitions]
        logger.info("Partitions assigned: %s", ", ".join(partition_strs))
        consumer.assign(partitions)
        self._consuming = True

    def _on_revoke(self, consumer, partitions):
        """
        Called when partitions are revoked.
        
        Runs in the Kafka polling thread - MUST be fast.
        Critical: No blocking calls allowed here.
        """
        partition_strs = [f"{tp.topic}-{tp.partition}" for tp in partitions]
        logger.info("Partitions revoked: %s", ", ".join(partition_strs))
        
        # Set flag - don't block waiting for inflight
        self._consuming = False
        
        # Commit offsets for revoked partitions
        # This is safe because we're in the Kafka thread
        try:
            if consumer and consumer.assignment():
                committable = self.offset_tracker.get_committable_offsets()
                if committable:
                    consumer.commit(offsets=committable, asynchronous=False)
                    logger.info("Offsets committed on revoke")
                    
                    # Mark as committed in tracker
                    for tp in committable:
                        self.offset_tracker.mark_committed(tp.topic, tp.partition, tp.offset)
        except Exception as e:
            logger.warning(f"Commit during revoke failed: {e}")
        
        # Clear partition tracking
        for tp in partitions:
            self.offset_tracker.clear_partition(tp.topic, tp.partition)

    # ---------- offset management ----------

    async def _commit_offsets(self):
        """Commit all pending offsets."""
        if not self._can_commit():
            return

        committable = self.offset_tracker.get_committable_offsets()
        if not committable:
            return

        try:
            # Run in thread executor to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self.consumer.commit(offsets=committable, asynchronous=False)
            )
            
            # Mark as committed
            for tp in committable:
                self.offset_tracker.mark_committed(tp.topic, tp.partition, tp.offset)
            
            logger.info(f"Committed {len(committable)} partition offsets")
            self._messages_since_commit = 0
            
        except Exception as e:
            logger.error(f"Offset commit failed: {e}", exc_info=True)

    # ---------- message processing ----------

    async def process_message(self, msg):
        """Process a single message from the queue."""
        if msg.error():
            logger.error(f"Message error: {msg.error()}")
            return

        self._inflight += 1
        try:
            if not self.processor:
                raise RuntimeError("Message processor not initialized")
            
            await self.processor.process_message(msg)
            
            # Check if we should commit
            self._messages_since_commit += 1
            if self._messages_since_commit >= COMMIT_INTERVAL_MESSAGES:
                await self._commit_offsets()
                
        finally:
            self._inflight -= 1

    # ---------- consuming loop ----------

    async def start_consuming(self):
        """
        Consume messages from the queue populated by the polling thread.
        
        This runs in the async event loop - all blocking I/O is in the thread.
        """
        if not self.message_queue:
            raise RuntimeError("Message queue not initialized")

        self._consuming = True
        logger.info("Kafka consumer started consuming from queue")

        while self._consuming:
            try:
                # Get message from queue with timeout
                msg = await asyncio.wait_for(
                    self.message_queue.get(),
                    timeout=1.0
                )
                await self.process_message(msg)
                
            except asyncio.TimeoutError:
                # No message in queue, continue
                continue
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
                
        logger.info("Consuming loop exited")

    # ---------- lag monitoring ----------

    async def _monitor_lag(self):
        """
        Background task to monitor consumer lag.
        
        Runs periodically instead of per-message to reduce overhead.
        """
        logger.info("Lag monitor task started")
        
        try:
            while self._consuming:
                await asyncio.sleep(LAG_MONITOR_INTERVAL_SEC)
                
                if not self.consumer or not self._consuming:
                    continue
                
                try:
                    # Run in executor to avoid blocking
                    loop = asyncio.get_event_loop()
                    assignments = await loop.run_in_executor(
                        None,
                        lambda: self.consumer.assignment() or []
                    )
                    
                    for tp in assignments:
                        try:
                            # Get committed offset and watermark
                            committed_list = await loop.run_in_executor(
                                None,
                                lambda: self.consumer.committed([tp], timeout=1.0)
                            )
                            committed = committed_list[0].offset if committed_list else None
                            
                            watermarks = await loop.run_in_executor(
                                None,
                                lambda: self.consumer.get_watermark_offsets(tp, timeout=1.0)
                            )
                            latest = watermarks[1]
                            
                            if committed is not None and latest is not None:
                                lag = max(0, latest - committed)
                                set_consumer_lag(tp.topic, tp.partition, lag)
                                
                        except Exception as e:
                            logger.debug(f"Failed to get lag for {tp.topic}-{tp.partition}: {e}")
                            
                except Exception as e:
                    logger.error(f"Lag monitoring error: {e}", exc_info=True)
                    
        except asyncio.CancelledError:
            logger.info("Lag monitor task cancelled")
            raise

    # ---------- healthcheck ----------

    async def healthcheck(self) -> bool:
        """Check consumer health."""
        return bool(self.consumer) and not self._closed


# =====================
# Entrypoint
# =====================

async def main():
    app = KafkaConsumerApp()
    
    try:
        await app.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await app.stop()
        await app.disconnect()


if __name__ == "__main__":
    asyncio.run(main())