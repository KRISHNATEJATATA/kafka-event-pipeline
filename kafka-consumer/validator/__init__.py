"""
Event Schema Validation Module

This module provides async validation for event messages against JSON schemas

"""
from validator.exceptions import ValidationError, SchemaNotFoundError
from validator.core import validate, validate_with_error

__all__ = [
    "validate",
    "validate_with_error",
    "ValidationError",
    "SchemaNotFoundError",
]

__version__ = "1.0.0"
