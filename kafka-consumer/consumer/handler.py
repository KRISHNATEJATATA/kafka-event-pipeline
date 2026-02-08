
import json
import logging
import uuid
from typing import Any
from .context import correlation_id_var, CorrelationIdFilter
from consumer.dlq import route_to_dlq
from consumer.enrich import enrich
from consumer.offset_manager import PartitionOffsetTracker
from consumer.retry_policy import NonRetryableError, retry_with_backoff
from validator.core import validate_with_error
from validator.exceptions import ValidationError

logger = logging.getLogger(__name__)
logger.addFilter(CorrelationIdFilter())


class KafkaMessageHandler:
    def __init__(self):
        self._failure_counts = {}

    async def handle_message(
        self,
        msg,
        correlation_id,
        value: dict[str, Any] | None = None,
    ) -> None:
        try:
            key = msg.key().decode() if msg.key() else None
            if value is None:
                raw_value = msg.value()
                if raw_value is None:
                    raise NonRetryableError("Message payload is empty")
                value = json.loads(raw_value.decode())
            # Set correlation ID in contextvar for this message
            token = correlation_id_var.set(correlation_id)
            try:
                # Track failure counts per key
                if key not in self._failure_counts:
                    self._failure_counts[key] = 0
                self._failure_counts[key] += 1

                # === BUSINESS LOGIC ENTRY POINT ===
                # await hero_usecase.process(value)
                # =================================

                logger.info(
                    f"Business logic completed "
                    f"(topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()})")
            finally:
                correlation_id_var.reset(token)
        except json.JSONDecodeError as e:
            raise NonRetryableError(f"Invalid JSON: {e}")


class KafkaMessageProcessor:
    def __init__(
        self,
        consumer,
        message_handler: KafkaMessageHandler,
        offset_tracker: PartitionOffsetTracker,
        max_retries: int,
        retry_backoff_sec: int,
    ) -> None:
        self.consumer = consumer
        self.message_handler = message_handler
        self.offset_tracker = offset_tracker
        self.max_retries = max_retries
        self.retry_backoff_sec = retry_backoff_sec

    async def process_message(self, msg):
        correlation_id = str(uuid.uuid4())
        try:
            raw_value = msg.value()
            if raw_value is None:
                raise ValueError("Message payload is empty")
            value = json.loads(raw_value.decode())
            correlation_id = value.get("correlation_id") or correlation_id
        except Exception as exc:
            reason = f"Invalid JSON payload: {exc}"
            logger.error(
                "Rejecting message due to payload decoding failure (topic=%s, partition=%s, offset=%s, correlation_id=%s): %s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                correlation_id,
                reason,
            )
            self._reject_message(msg, correlation_id, reason)
            return

        # Call enrich before validation
        enriched_value = enrich(value)
        validated_value, validation_error = await validate_with_error(enriched_value)
        if validated_value is None:
            reason = self._format_validation_error(validation_error)
            logger.error(
                "Rejecting message due to validation failure (topic=%s, partition=%s, offset=%s, correlation_id=%s): %s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                correlation_id,
                reason,
            )
            self._reject_message(msg, correlation_id, reason)
            return

        async def _handle():
            await self.message_handler.handle_message(
                msg,
                correlation_id,
                validated_value,
            )

        success, error = await retry_with_backoff(
            _handle,
            max_retries=self.max_retries,
            backoff_sec=self.retry_backoff_sec,
        )

        if success:
            # Record as processed - actual commit happens in batch
            self.offset_tracker.record_processed(
                msg.topic(), msg.partition(), msg.offset()
            )
            logger.debug(
                "Message processed successfully",
                extra={
                    "correlation_id": correlation_id,
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                }
            )
            return

        if isinstance(error, NonRetryableError):
            logger.error(
                "Dropping message due to non-retryable error",
                extra={
                    "correlation_id": correlation_id,
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "error": str(error),
                }
            )
            # Record as processed even though it failed (don't retry non-retryable)
            self.offset_tracker.record_processed(
                msg.topic(), msg.partition(), msg.offset()
            )
            return

        logger.error(
            "Message processing failed after retries",
            extra={
                "correlation_id": correlation_id,
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "error": str(error),
            }
        )
        dlq_success = route_to_dlq(msg, str(error))
        if dlq_success:
            logger.info(
                "Message routed to DLQ",
                extra={
                    "correlation_id": correlation_id,
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                }
            )
            # Record as processed after DLQ routing
            self.offset_tracker.record_processed(
                msg.topic(), msg.partition(), msg.offset()
            )
            return

        logger.error(
            "DLQ routing failed - recording offset to avoid infinite retry loop",
            extra={
                "correlation_id": correlation_id,
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
            }
        )
        # Record as processed to avoid infinite retry
        self.offset_tracker.record_processed(
            msg.topic(), msg.partition(), msg.offset()
        )

    def _reject_message(self, msg, correlation_id: str, reason: str) -> None:
        dlq_success = route_to_dlq(msg, reason)
        if dlq_success:
            logger.info(
                "Message routed to DLQ",
                extra={
                    "correlation_id": correlation_id,
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                }
            )
        else:
            logger.error(
                "DLQ routing failed - recording offset to avoid infinite retry loop",
                extra={
                    "correlation_id": correlation_id,
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                }
            )

        # Record as processed
        self.offset_tracker.record_processed(
            msg.topic(), msg.partition(), msg.offset()
        )

    @staticmethod
    def _format_validation_error(error: ValidationError | None) -> str:
        if error is None:
            return "Message failed validation"

        code_prefix = f"[{error.code}] " if getattr(error, "code", None) else ""
        details_suffix = f" details={error.details}" if error.details else ""
        return f"{code_prefix}{error.message}{details_suffix}"
