"""
Schema loader with caching for JSON schema validation.

This module loads and caches JSON schemas from the filesystem,
creating compiled jsonschema validators for optimal performance.
"""

import json
from pathlib import Path
from typing import Any

from jsonschema import Draft7Validator, ValidationError as JsonSchemaValidationError
from jsonschema.validators import validator_for

from validator.constants import EVENT_TYPE_TO_SCHEMA, ALL_EVENT_TYPES
from validator.exceptions import SchemaNotFoundError


class SchemaLoader:
    """
    Loads and caches JSON schemas for event validation.
    
    This class provides:
    - Lazy loading of schemas on first access
    - Compiled validator caching for performance
    - Support for both envelope (base-event) and payload schemas
    
    Attributes:
        schemas_dir: Path to the root schemas directory
    """
    
    def __init__(self, schemas_dir: str | Path):
        """
        Initialize the schema loader.
        
        Args:
            schemas_dir: Path to the schemas directory containing
                         'envelope/' and 'payload/' subdirectories
        """
        self.schemas_dir = Path(schemas_dir)
        self._envelope_dir = self.schemas_dir / "envelope"
        self._payload_dir = self.schemas_dir / "payload"
        
        # Cache for loaded schemas (raw JSON)
        self._schema_cache: dict[str, dict[str, Any]] = {}
        
        # Cache for compiled validators
        self._validator_cache: dict[str, Draft7Validator] = {}
    
    def _load_schema_file(self, schema_path: Path) -> dict[str, Any]:
        """
        Load a JSON schema file from disk.
        
        Args:
            schema_path: Path to the schema file
            
        Returns:
            Parsed JSON schema as a dictionary
            
        Raises:
            SchemaNotFoundError: If the file doesn't exist or can't be parsed
        """
        cache_key = str(schema_path)
        
        if cache_key in self._schema_cache:
            return self._schema_cache[cache_key]
        
        if not schema_path.exists():
            raise SchemaNotFoundError(
                event_type=schema_path.stem,
                schema_path=str(schema_path)
            )
        
        try:
            with open(schema_path, "r", encoding="utf-8") as f:
                schema = json.load(f)
        except json.JSONDecodeError as e:
            raise SchemaNotFoundError(
                event_type=schema_path.stem,
                schema_path=f"{schema_path} (invalid JSON: {e})"
            )
        
        self._schema_cache[cache_key] = schema
        return schema
    
    def _get_validator(self, schema_path: Path) -> Draft7Validator:
        """
        Get a compiled validator for the given schema, with caching.
        
        Args:
            schema_path: Path to the schema file
            
        Returns:
            Compiled Draft7Validator instance
        """
        cache_key = str(schema_path)
        
        if cache_key in self._validator_cache:
            return self._validator_cache[cache_key]
        
        schema = self._load_schema_file(schema_path)
        
        # Use the validator class specified in the schema, default to Draft7
        validator_cls = validator_for(schema)
        
        # Check schema validity
        validator_cls.check_schema(schema)
        
        # Create and cache the validator
        validator = validator_cls(schema)
        self._validator_cache[cache_key] = validator
        
        return validator
    
    def get_base_event_validator(self) -> Draft7Validator:
        """
        Get the validator for the base event envelope schema.
        
        Returns:
            Compiled validator for envelope.json
        """
        schema_path = self._envelope_dir / "envelope.json"
        return self._get_validator(schema_path)
    
    def get_payload_validator(self, event_type: str) -> Draft7Validator:
        """
        Get the validator for a specific event type's payload schema.
        
        Args:
            event_type: The event type (e.g., 'asset_created')
            
        Returns:
            Compiled validator for the event's payload schema
            
        Raises:
            SchemaNotFoundError: If the event type is unknown or schema missing
        """
        if event_type not in EVENT_TYPE_TO_SCHEMA:
            raise SchemaNotFoundError(
                event_type=event_type,
                schema_path=f"Unknown event type: {event_type}"
            )
        
        schema_filename = EVENT_TYPE_TO_SCHEMA[event_type]
        schema_path = self._payload_dir / schema_filename
        
        return self._get_validator(schema_path)
    
    def get_payload_schema(self, event_type: str) -> dict[str, Any]:
        """
        Get the raw payload schema for a specific event type.
        
        Args:
            event_type: The event type (e.g., 'asset_created')
            
        Returns:
            Raw schema dictionary
            
        Raises:
            SchemaNotFoundError: If the event type is unknown or schema missing
        """
        if event_type not in EVENT_TYPE_TO_SCHEMA:
            raise SchemaNotFoundError(
                event_type=event_type,
                schema_path=f"Unknown event type: {event_type}"
            )
        
        schema_filename = EVENT_TYPE_TO_SCHEMA[event_type]
        schema_path = self._payload_dir / schema_filename
        
        return self._load_schema_file(schema_path)
    
    def validate_envelope(self, message: dict[str, Any]) -> list[str]:
        """
        Validate a message against the base event envelope schema.
        
        Args:
            message: The event message to validate
            
        Returns:
            List of validation error messages (empty if valid)
        """
        validator = self.get_base_event_validator()
        errors = list(validator.iter_errors(message))
        return [self._format_error(e) for e in errors]
    
    def validate_payload(
        self, 
        event_type: str, 
        payload: dict[str, Any]
    ) -> list[str]:
        """
        Validate a payload against its event type's schema.
        
        Args:
            event_type: The event type
            payload: The payload object to validate
            
        Returns:
            List of validation error messages (empty if valid)
        """
        validator = self.get_payload_validator(event_type)
        errors = list(validator.iter_errors(payload))
        return [self._format_error(e) for e in errors]
    
    @staticmethod
    def _format_error(error: JsonSchemaValidationError) -> str:
        """Format a jsonschema ValidationError into a readable string."""
        path = ".".join(str(p) for p in error.absolute_path) if error.absolute_path else "root"
        return f"{path}: {error.message}"
    
    def preload_all_schemas(self) -> None:
        """
        Preload and cache all schemas for faster validation.
        
        This method loads the base event schema and all 18 payload schemas
        into the cache. Call this at application startup for optimal
        runtime performance.
        """
        # Load base event schema
        self.get_base_event_validator()
        
        # Load all payload schemas
        for event_type in ALL_EVENT_TYPES:
            try:
                self.get_payload_validator(event_type)
            except SchemaNotFoundError:
                # Log warning but continue loading other schemas
                pass
    
    def clear_cache(self) -> None:
        """Clear all cached schemas and validators."""
        self._schema_cache.clear()
        self._validator_cache.clear()


# Module-level singleton for convenience
_default_loader: SchemaLoader | None = None


def get_schema_loader(schemas_dir: str | Path | None = None) -> SchemaLoader:
    """
    Get the default schema loader singleton.
    
    Args:
        schemas_dir: Path to schemas directory (required on first call)
        
    Returns:
        SchemaLoader singleton instance
    """
    global _default_loader
    
    if _default_loader is None:
        if schemas_dir is None:
            raise ValueError("schemas_dir must be provided on first call")
        _default_loader = SchemaLoader(schemas_dir)
    
    return _default_loader


def reset_schema_loader() -> None:
    """Reset the default schema loader (useful for testing)."""
    global _default_loader
    _default_loader = None
