"""Custom exceptions for event validation."""



class ValidationError(Exception):
    """
    Raised when an event message fails validation.
    
    Attributes:
        code: Error code identifying the type of validation failure
        message: Human-readable error description
        details: Additional context about the failure (optional)
    """
    
    def __init__(self, code: str, message: str, details: dict | None = None):
        self.code = code
        self.message = message
        self.details = details or {}
        super().__init__(f"[{code}] {message}")
    
    def to_dict(self) -> dict:
        """Convert exception to a dictionary for logging/serialization."""
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


class SchemaNotFoundError(Exception):
    """
    Raised when a schema file cannot be found or loaded.
    
    Attributes:
        event_type: The event type whose schema was not found
        schema_path: The path that was searched
    """
    
    def __init__(self, event_type: str, schema_path: str | None = None):
        self.event_type = event_type
        self.schema_path = schema_path
        message = f"Schema not found for event type: {event_type}"
        if schema_path:
            message += f" (searched: {schema_path})"
        super().__init__(message)
