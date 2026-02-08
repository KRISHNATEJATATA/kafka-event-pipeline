from datetime import datetime, timezone
from typing import Any


def enrich(msg: dict[str, Any]) -> dict[str, Any]:
    if "eventTime" not in msg or msg["eventTime"] is None:
        msg["eventTime"] = datetime.now(timezone.utc).isoformat()
    return msg