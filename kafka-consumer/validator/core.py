"""Core async validation function for event messages.

This module provides the main validate() function that:
1. Validates messages against JSON schemas
2. Enforces business rules
"""

import logging
from pathlib import Path
from typing import Any, Tuple

from validator.constants import (
    ALL_EVENT_TYPES,
    ErrorCodes,
)
from validator.exceptions import ValidationError, SchemaNotFoundError
from validator.schema_loader import get_schema_loader

logger = logging.getLogger(__name__)


def _get_default_schemas_dir() -> Path:
    """Get the default schemas directory relative to this module."""
    module_dir = Path(__file__).parent
    return module_dir.parent / "schemas"


def _validate_event_type(event_type: str | None) -> ValidationError | None:
    """
    Validate that event_type is known.
    """
    if event_type is None:
        return ValidationError(
            code=ErrorCodes.MISSING_REQUIRED_FIELD,
            message="event_type is required but was not provided",
            details={"field": "event_type"}
        )
    
    if event_type not in ALL_EVENT_TYPES:
        return ValidationError(
            code=ErrorCodes.UNKNOWN_EVENT_TYPE,
            message=f"Unknown event type: '{event_type}'",
            details={
                "field": "event_type",
                "value": event_type,
                "allowed_values": list(ALL_EVENT_TYPES)
            }
        )
    
    return None


def _schema_validation_error(stage: str, errors: list[str]) -> ValidationError:
    return ValidationError(
        code=ErrorCodes.INVALID_SCHEMA,
        message=f"{stage} schema validation failed",
        details={"errors": errors},
    )


def _loader_init_error(exc: Exception) -> ValidationError:
    return ValidationError(
        code=ErrorCodes.SCHEMA_LOAD_ERROR,
        message=f"Failed to initialize schema loader: {exc}",
    )


def _schema_not_found_error(event_type: str, exc: SchemaNotFoundError) -> ValidationError:
    return ValidationError(
        code=ErrorCodes.SCHEMA_LOAD_ERROR,
        message=f"Schema not found for {event_type}: {exc}",
        details={
            "event_type": event_type,
            "schema_path": getattr(exc, "schema_path", None),
        },
    )


def _perform_validation(
    msg: dict[str, Any],
    schemas_dir: str | Path | None = None,
) -> Tuple[dict[str, Any] | None, ValidationError | None]:
    if schemas_dir is None:
        schemas_dir = _get_default_schemas_dir()

    try:
        loader = get_schema_loader(schemas_dir)
    except Exception as exc:  # pragma: no cover - defensive guard
        error = _loader_init_error(exc)
        logger.error(error.message)
        return None, error

    event_type = msg.get("eventType")

    error = _validate_event_type(event_type)
    if error:
        logger.warning(f"Event type validation failed: {error}")
        return None, error

    envelope_errors = loader.validate_envelope(msg)
    if envelope_errors:
        error = _schema_validation_error("Envelope", envelope_errors)
        logger.warning(f"Envelope schema validation failed: {'; '.join(envelope_errors)}")
        return None, error

    payload = msg.get("payload")
    if payload is not None and event_type:
        try:
            payload_errors = loader.validate_payload(event_type, payload)
            if payload_errors:
                error = _schema_validation_error(
                    f"Payload ({event_type})",
                    payload_errors,
                )
                logger.warning(
                    "Payload schema validation failed for %s: %s",
                    event_type,
                    "; ".join(payload_errors),
                )
                return None, error
        except SchemaNotFoundError as exc:
            error = _schema_not_found_error(event_type, exc)
            logger.error(error.message)
            return None, error

    logger.debug(f"Message validated successfully: eventType={event_type}")
    return msg, None


async def validate(
    msg: dict[str, Any],
    schemas_dir: str | Path | None = None,
) -> dict[str, Any] | None:
    """Validate an event message.

    Returns the validated message if successful, otherwise None.
    """
    result, _ = _perform_validation(msg, schemas_dir)
    return result


async def validate_with_error(
    msg: dict[str, Any],
    schemas_dir: str | Path | None = None,
) -> tuple[dict[str, Any] | None, ValidationError | None]:
    """Validate an event message and return detailed errors."""
    return _perform_validation(msg, schemas_dir)


async def validate_batch(
    messages: list[dict[str, Any]],
    schemas_dir: str | Path | None = None,
) -> list[dict[str, Any]]:
    """
    Validate a batch of messages, returning only valid ones.
    """
    valid_messages = []
    
    for msg in messages:
        result = await validate(msg, schemas_dir)
        if result is not None:
            valid_messages.append(result)
    
    return valid_messages
