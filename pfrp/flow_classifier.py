"""FlowClassifier: decides single-channel vs multi-channel mode per stream."""

from collections import deque
import time
from typing import Dict, List, Tuple

from pfrp.channel_monitor import ChannelQualityMonitor
from pfrp.constants import (
    FLOW_RATE_THRESHOLD_MIN,
    FLOW_RATE_THRESHOLD_MAX,
    FLOW_BYTES_THRESHOLD_MULTIPLIER,
    FLOW_PROMOTION_WINDOW,
)


class FlowClassifier:
    """Classifies streams into single-channel or multi-channel mode.

    Uses adaptive thresholds based on total bandwidth observed by the
    ChannelQualityMonitor.
    """

    MODE_SINGLE = 1
    MODE_MULTI = 2

    def __init__(self, monitor: ChannelQualityMonitor):
        self._monitor = monitor
        # stream_id -> dict with state
        self._streams: Dict[str, dict] = {}

    def set_mode(self, stream_id: str, mode: int) -> None:
        """Set the mode for a stream.

        Args:
            stream_id: Unique identifier for the stream.
            mode: MODE_SINGLE or MODE_MULTI.
        """
        state = self._get_stream_state(stream_id)
        state['mode'] = mode

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_rate_threshold(self) -> float:
        """Adaptive rate threshold in bytes per second."""
        total_mbps = self._monitor.total_throughput_mbps
        # total_throughput_mbps is in megabits per second
        # Convert to bytes per second: Mbps * 1_000_000 / 8 = bytes/s
        total_bytes_per_sec = (total_mbps * 1_000_000.0) / 8.0
        adaptive = total_bytes_per_sec * 0.05
        return max(FLOW_RATE_THRESHOLD_MIN, min(FLOW_RATE_THRESHOLD_MAX, adaptive))

    def _get_bytes_threshold(self) -> float:
        """Adaptive bytes threshold."""
        return self._get_rate_threshold() * FLOW_BYTES_THRESHOLD_MULTIPLIER

    def _get_stream_state(self, stream_id: str) -> dict:
        if stream_id not in self._streams:
            self._streams[stream_id] = {
                "mode": self.MODE_SINGLE,
                "total_bytes": 0,
                "samples": deque(),  # (timestamp, bytes_count)
            }
        return self._streams[stream_id]

    def _check_promotion(self, state: dict, timestamp: float) -> bool:
        """Check if a stream should be promoted to multi-channel mode."""
        # Check bytes threshold
        if state["total_bytes"] >= self._get_bytes_threshold():
            return True

        # Check sustained rate threshold
        samples = state["samples"]
        if not samples:
            return False

        # Trim old samples outside the window (amortized O(1) with deque)
        window_start = timestamp - FLOW_PROMOTION_WINDOW
        while samples and samples[0][0] < window_start:
            samples.popleft()

        if not samples:
            return False

        total_bytes_in_window = sum(bc for _, bc in samples)
        window_duration = max(samples[-1][0] - samples[0][0], 1e-9)
        rate = total_bytes_in_window / window_duration

        return rate >= self._get_rate_threshold()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record_bytes(self, stream_id: str, bytes_count: int, timestamp: float) -> None:
        """Track data transfer for a stream.

        Args:
            stream_id: Unique identifier for the stream.
            bytes_count: Number of bytes transferred.
            timestamp: Unix timestamp of the transfer.
        """
        state = self._get_stream_state(stream_id)

        if state["mode"] == self.MODE_MULTI:
            # Already promoted, no need to track further
            return

        state["total_bytes"] += bytes_count
        state["samples"].append((timestamp, bytes_count))

        if self._check_promotion(state, timestamp):
            state["mode"] = self.MODE_MULTI

    def get_mode(self, stream_id: str) -> int:
        """Return the current mode for a stream.

        Args:
            stream_id: Unique identifier for the stream.

        Returns:
            MODE_SINGLE or MODE_MULTI.
        """
        state = self._get_stream_state(stream_id)
        return state["mode"]

    def remove_stream(self, stream_id: str) -> None:
        """Clean up state for a stream.

        Args:
            stream_id: Unique identifier for the stream.
        """
        if stream_id in self._streams:
            del self._streams[stream_id]
