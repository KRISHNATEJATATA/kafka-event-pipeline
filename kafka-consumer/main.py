import asyncio
import logging
import signal
import sys
import os
from container import container


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class WorkerApp:
    def __init__(self):
        self.consumer = None
        self.running = False
        self.shutdown_event = asyncio.Event()

    def setup_signal_handlers(self):
        def handler(signum, frame):
            logger.info(f"Signal {signum} received, initiating shutdown")
            self.shutdown_event.set()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

    async def log_health_status(self):
        """
        Log health status periodically (no HTTP endpoints)
        Uses container.health_check()
        """
        try:
            health = await container.health_check()

            logger.info(f"Health status: {health.get('status')}")

            components = health.get("components", {})
            for component, status in components.items():
                logger.info(f"  {component}: {status}")

        # Optional: surface error details if unhealthy
            if health.get("status") != "healthy" and "error" in health:
                logger.warning(f"Health check error: {health['error']}")
        except Exception as e:
            logger.error("Health check failed", exc_info=e)
            raise
    async def start_consumer(self):
        """Start Kafka consumer"""
        try:
            self.consumer = await container.get_consumer()

            # Start Kafka consumer as background asyncio task
            consumer_task = asyncio.create_task(self.consumer.start())
            logger.info("Kafka consumer started")

            return consumer_task

        except Exception as e:
            logger.error(f"Failed to start Kafka consumer: {e}", exc_info=True)
            raise

    async def run(self):
        logger.info("Starting Worker Service")
        logger.info(f"Environment: {os.getenv('APP_ENV', 'development')}")
        try:
            self.setup_signal_handlers()
            await self.log_health_status()
            # Create topics if needed
            await container.create_topics()
            consumer_task = await self.start_consumer()
            self.running = True
            logger.info("Worker application started successfully")
            
            # Wait for either consumer task to complete or shutdown signal
            done, pending = await asyncio.wait(
                [consumer_task, asyncio.create_task(self.shutdown_event.wait())],
                return_when=asyncio.FIRST_COMPLETED,
            )
            
            # Check which task completed
            if self.shutdown_event.is_set():
                # Shutdown signal received - graceful stop
                logger.info("Shutdown signal received, stopping consumer gracefully...")
                
                # Signal consumer to stop (non-blocking flag set)
                if self.consumer:
                    await self.consumer.stop()
                
                # Now wait for consumer task to finish gracefully
                logger.info("Waiting for consumer task to complete...")
                try:
                    await asyncio.wait_for(consumer_task, timeout=30.0)
                    logger.info("Consumer task completed gracefully")
                except asyncio.TimeoutError:
                    logger.warning("Consumer task did not complete within timeout")
                    consumer_task.cancel()
                    try:
                        await consumer_task
                    except asyncio.CancelledError:
                        pass
            else:
                # Consumer task completed unexpectedly
                logger.warning("Consumer task completed unexpectedly")
                # Cancel shutdown event waiter
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        except Exception as e:
            logger.error(f"Worker application error: {e}")
            raise

        finally:
            await self.shutdown()

    async def shutdown(self):
        """Clean shutdown of worker application.
        
        This is the ONLY place that orchestrates consumer shutdown.
        Ensures no double-shutdown or resource conflicts.
        """
        logger.info("Shutting down worker application")
        try:
            if self.consumer:
                # Disconnect handles final cleanup
                await self.consumer.disconnect()
            await container.close()
            self.running = False
            logger.info("Worker shutdown complete")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    


async def main():
    """Main entry point"""
    worker_app = WorkerApp()

    try:
        await worker_app.run()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Application error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
