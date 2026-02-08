
import logging
import time
from dlqproducer import send_to_dlq
from .context import correlation_id_var, CorrelationIdFilter

logger = logging.getLogger(__name__)
logger.addFilter(CorrelationIdFilter())

# Patch all log handlers to include correlation_id in their formatter
root_logger = logging.getLogger()
for handler in root_logger.handlers:
    if hasattr(handler, 'formatter') and handler.formatter:
        fmt = handler.formatter._fmt
        if '%(correlation_id)' not in fmt:
            handler.setFormatter(logging.Formatter(fmt + ' [correlation_id=%(correlation_id)s]'))
    else:
        # If no formatter is set, set a default one that includes correlation_id
        handler.setFormatter(logging.Formatter('%(levelname)s %(name)s:%(lineno)d %(message)s [correlation_id=%(correlation_id)s]'))

def route_to_dlq(msg, error: str) -> bool:
    # Try to extract correlation_id from message headers if available
    correlation_id = None
    if hasattr(msg, 'headers') and msg.headers():
        for k, v in msg.headers():
            if k.lower() == 'correlation_id' and v:
                correlation_id = v.decode() if isinstance(v, bytes) else v
                break
    token = correlation_id_var.set(correlation_id)
    try:
        payload = {
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "key": msg.key().decode() if msg.key() else None,
        "error": error,
        "timestamp": time.time(),
        }
        send_to_dlq(payload)
        logger.error(
            f"Message sent to DLQ "
            f"(topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()})")
        return True
    except Exception as e:
        logger.critical("DLQ publish failed offset NOT committed", exc_info=e)
        return False
    finally:
        correlation_id_var.reset(token)

