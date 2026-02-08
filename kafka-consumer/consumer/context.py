import contextvars
import logging

# Shared ContextVar for correlation ID
correlation_id_var = contextvars.ContextVar("correlation_id", default=None)

class CorrelationIdFilter(logging.Filter):
    def filter(self, record):
        cid = correlation_id_var.get()
        if cid is None:
            import uuid
            cid = str(uuid.uuid4())
            correlation_id_var.set(cid)
        record.correlation_id = cid
        return True
