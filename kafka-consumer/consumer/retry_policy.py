import asyncio
import logging

logger = logging.getLogger(__name__)

async def retry_with_backoff(
    func,
    *,
    max_retries: int,
    backoff_sec: int,
):
    attempt = 0
    while attempt < max_retries:
        try:
            return True, await func()
        except NonRetryableError as nre:
            logger.error(f"Non-retryable error encountered: {nre}")
            # Immediately return failure, do not retry
            return False, nre
        except Exception as e:
            attempt += 1
            logger.error(f"Attempt {attempt} failed: {e}")
            if attempt >= max_retries:
                return False, e
            await asyncio.sleep(backoff_sec)
        
class NonRetryableError(Exception):
    pass

