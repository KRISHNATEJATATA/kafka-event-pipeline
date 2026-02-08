"""Constants for event validation."""



# All supported event types
ALL_EVENT_TYPES: tuple[str, ...] = ("user-created","user-deleted","file-added","file-removed")

# Mapping from event_type to payload schema filename
EVENT_TYPE_TO_SCHEMA: dict[str, str] = {
    "user-created": "user-created.json",
    "user-deleted": "user-deleted.json",
    "file-added": "file-added.json",
    "file-removed": "file-removed.json",
}

# Error codes for validation failures
class ErrorCodes:
    """Standard error codes for validation failures."""
    MISSING_USER_ID = "MISSING_USER_ID"
    INVALID_SCHEMA = "INVALID_SCHEMA"
    UNKNOWN_EVENT_TYPE = "UNKNOWN_EVENT_TYPE"
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
    SCHEMA_LOAD_ERROR = "SCHEMA_LOAD_ERROR"
