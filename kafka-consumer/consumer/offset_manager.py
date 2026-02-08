"""
Partition-aware offset tracking and management.

Prevents offset commit races by tracking the highest contiguous offset
per partition and only committing safe offsets.
"""

import logging
from collections import defaultdict
from typing import Dict, Tuple
from confluent_kafka import TopicPartition

logger = logging.getLogger(__name__)


class PartitionOffsetTracker:
    """
    Tracks processed offsets per partition to prevent commit races.
    
    Problem:
        Multiple async tasks processing messages from the same partition
        can finish out of order, causing offset rewinds:
        - msg offset 10 finishes → commit 10
        - msg offset 8 finishes later → commit 8 ❌ rewind
    
    Solution:
        Track all processed offsets and only commit the highest
        contiguous offset (no gaps).
    """

    def __init__(self):
        # Track highest contiguous offset per (topic, partition)
        self._committed: Dict[Tuple[str, int], int] = {}
        
        # Track processed offsets that haven't been committed yet
        self._processed: Dict[Tuple[str, int], set] = defaultdict(set)

    def record_processed(self, topic: str, partition: int, offset: int):
        """
        Record that a message has been successfully processed.
        
        Args:
            topic: Kafka topic name
            partition: Partition number
            offset: Message offset
        """
        key = (topic, partition)
        self._processed[key].add(offset)
        logger.debug(f"Recorded processed offset: {topic}-{partition} @ {offset}")

    def get_committable_offsets(self) -> list[TopicPartition]:
        """
        Get the list of offsets that can be safely committed.
        
        Returns only the highest contiguous offset per partition
        (no gaps in the sequence).
        
        Returns:
            List of TopicPartition objects ready to commit
        """
        committable = []

        for (topic, partition), processed_offsets in self._processed.items():
            if not processed_offsets:
                continue

            # Get the last committed offset (or -1 if never committed)
            last_committed = self._committed.get((topic, partition), -1)

            # Find the highest contiguous offset from last_committed
            sorted_offsets = sorted(processed_offsets)
            highest_contiguous = last_committed

            for offset in sorted_offsets:
                # Offsets must be contiguous (next offset = current + 1)
                if offset == highest_contiguous + 1:
                    highest_contiguous = offset
                elif offset > highest_contiguous + 1:
                    # Gap detected, stop here
                    break

            # If we found new contiguous offsets, mark for commit
            if highest_contiguous > last_committed:
                # Kafka commits the "next offset to read", so we add 1
                tp = TopicPartition(topic, partition, highest_contiguous + 1)
                committable.append(tp)

        return committable

    def mark_committed(self, topic: str, partition: int, offset: int):
        """
        Mark an offset as committed and clean up processed offsets.
        
        Args:
            topic: Kafka topic name
            partition: Partition number
            offset: Committed offset (this is the "next offset to read")
        """
        key = (topic, partition)
        
        # Update committed offset (Kafka offset is "next to read", so actual is offset - 1)
        actual_offset = offset - 1
        self._committed[key] = actual_offset

        # Remove all processed offsets <= actual_offset
        if key in self._processed:
            self._processed[key] = {
                o for o in self._processed[key] if o > actual_offset
            }

        logger.debug(f"Marked committed: {topic}-{partition} @ {actual_offset}")

    def clear_partition(self, topic: str, partition: int):
        """
        Clear tracking for a partition (called on revoke).
        
        Args:
            topic: Kafka topic name
            partition: Partition number
        """
        key = (topic, partition)
        self._processed.pop(key, None)
        # Keep committed offset for future reference
        logger.debug(f"Cleared partition tracking: {topic}-{partition}")

    def has_uncommitted(self) -> bool:
        """
        Check if there are any uncommitted processed offsets.
        
        Returns:
            True if any partition has processed but uncommitted offsets
        """
        return any(bool(offsets) for offsets in self._processed.values())

    def get_partition_info(self, topic: str, partition: int) -> dict:
        """
        Get debug info for a partition.
        
        Returns:
            Dictionary with committed offset and pending offsets
        """
        key = (topic, partition)
        return {
            "committed": self._committed.get(key, -1),
            "processed_pending": sorted(self._processed.get(key, set())),
        }


def commit_offset(consumer, msg):
    """
    Legacy single-message commit (deprecated).
    
    Use PartitionOffsetTracker for production code.
    This is kept for backward compatibility during migration.
    """
    try:
        consumer.commit(message=msg, asynchronous=False)
        logger.debug(
            f"Committed offset: topic={msg.topic()}, "
            f"partition={msg.partition()}, offset={msg.offset()}"
        )
    except Exception as e:
        logger.critical(
            f"Offset commit failed "
            f"(topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()})",
            exc_info=e,
        )
        raise
