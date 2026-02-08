"""
Thread-based Kafka I/O to prevent blocking the async event loop.

confluent_kafka.Consumer is synchronous and blocks on all operations.
Running it in a dedicated thread ensures:
- Predictable heartbeats
- No event loop starvation
- Clean shutdown semantics
"""

import asyncio
import logging
import threading
from typing import Optional
from confluent_kafka import Consumer, KafkaException

logger = logging.getLogger(__name__)


class KafkaPollingThread:
    """
    Dedicated thread for Kafka polling operations.
    
    Runs consumer.consume() in a loop and pushes messages to an asyncio.Queue.
    All blocking Kafka operations run in this thread, not the event loop.
    """

    def __init__(
        self,
        consumer: Consumer,
        message_queue: asyncio.Queue,
        max_batch_size: int = 100,
        poll_timeout_sec: float = 1.0,
    ):
        self.consumer = consumer
        self.message_queue = message_queue
        self.max_batch_size = max_batch_size
        self.poll_timeout_sec = poll_timeout_sec

        self._thread: Optional[threading.Thread] = None
        self._running = threading.Event()
        self._stopped = threading.Event()
        self._exception: Optional[Exception] = None

    def start(self):
        """Start the polling thread."""
        if self._thread is not None:
            raise RuntimeError("Polling thread already started")

        self._running.set()
        self._thread = threading.Thread(target=self._poll_loop, daemon=False)
        self._thread.start()
        logger.info("Kafka polling thread started")

    def stop(self):
        """Signal the thread to stop gracefully."""
        logger.info("Signaling Kafka polling thread to stop...")
        self._running.clear()

    def join(self, timeout: Optional[float] = None):
        """Wait for the thread to finish."""
        if self._thread:
            self._thread.join(timeout=timeout)
            logger.info("Kafka polling thread joined")

    def is_alive(self) -> bool:
        """Check if the thread is still running."""
        return self._thread is not None and self._thread.is_alive()

    def get_exception(self) -> Optional[Exception]:
        """Get any exception that occurred in the thread."""
        return self._exception

    def _poll_loop(self):
        """
        Main polling loop running in the thread.
        
        This is the ONLY place where blocking Kafka calls should happen.
        """
        try:
            logger.info("Kafka polling loop started")
            while self._running.is_set():
                try:
                    # Blocking call - safe because we're in a dedicated thread
                    messages = self.consumer.consume(
                        num_messages=self.max_batch_size,
                        timeout=self.poll_timeout_sec,
                    )

                    if not messages:
                        continue

                    # Push messages to async queue
                    # Use thread-safe call_soon_threadsafe
                    for msg in messages:
                        if msg.error():
                            logger.error(f"Kafka message error: {msg.error()}")
                            continue
                        
                        # Put message in queue for async processing
                        # This is thread-safe
                        asyncio.run_coroutine_threadsafe(
                            self.message_queue.put(msg),
                            asyncio.get_event_loop()
                        )

                except KafkaException as e:
                    logger.error(f"Kafka polling error: {e}", exc_info=True)
                    self._exception = e
                    break
                except Exception as e:
                    logger.error(f"Unexpected error in polling loop: {e}", exc_info=True)
                    self._exception = e
                    break

            logger.info("Kafka polling loop exited")
        finally:
            self._stopped.set()
