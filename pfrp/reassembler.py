"""SequenceReassembler: reassemble out-of-order chunks from multiple channels."""

import logging
import time
from typing import Dict, List, Tuple

from pfrp.constants import REASSEMBLER_MAX_BUFFER, REASSEMBLER_TIMEOUT_MS

logger = logging.getLogger(__name__)


class SequenceReassembler:
    """Receives chunks arriving out of order and reassembles them in sequence order.

    Attributes:
        _next_seq: The next expected sequence number.
        _buffer: Dict mapping sequence number -> bytes for out-of-order chunks.
        _buffered_size: Total byte size of buffered chunks.
        _last_receive_time: Timestamp (monotonic) of the last receive() call.
    """

    def __init__(self):
        self._next_seq: int = 0
        self._buffer: Dict[int, bytes] = {}
        self._buffered_size: int = 0
        self._last_receive_time: float = 0.0

    def receive(self, seq: int, data: bytes) -> List[Tuple[int, bytes]]:
        """Receive a chunk and return any consecutive chunks starting from next_seq.

        Args:
            seq: Sequence number of the chunk.
            data: Chunk payload.

        Returns:
            List of (seq, data) tuples that are now consecutive from next_seq.
        """
        self._last_receive_time = time.monotonic()

        # Ignore already delivered chunks
        if seq < self._next_seq:
            return []

        # If this chunk is the next expected, return it and any buffered consecutive chunks
        if seq == self._next_seq:
            self._next_seq += 1
            result: List[Tuple[int, bytes]] = [(seq, data)]
            while self._next_seq in self._buffer:
                buffered_data = self._buffer.pop(self._next_seq)
                self._buffered_size -= len(buffered_data)
                result.append((self._next_seq, buffered_data))
                self._next_seq += 1
            return result

        # Out-of-order chunk: buffer it
        if seq in self._buffer:
            # Duplicate in buffer, ignore
            return []

        # Enforce memory limit
        if self._buffered_size + len(data) > REASSEMBLER_MAX_BUFFER:
            logger.warning(
                "Reassembler buffer limit exceeded (%d > %d), dropping seq=%d",
                self._buffered_size + len(data),
                REASSEMBLER_MAX_BUFFER,
                seq,
            )
            return []

        self._buffer[seq] = data
        self._buffered_size += len(data)
        return []

    def is_stalled(self, timeout_ms: int = REASSEMBLER_TIMEOUT_MS) -> bool:
        """Check if waiting too long for a missing sequence number.

        Args:
            timeout_ms: Timeout in milliseconds.

        Returns:
            True if there are buffered chunks and time since last receive exceeds timeout.
        """
        if not self._buffer:
            return False
        elapsed_ms = (time.monotonic() - self._last_receive_time) * 1000.0
        return elapsed_ms > timeout_ms

    def reset(self) -> None:
        """Clear all state."""
        self._next_seq = 0
        self._buffer.clear()
        self._buffered_size = 0
        self._last_receive_time = 0.0
