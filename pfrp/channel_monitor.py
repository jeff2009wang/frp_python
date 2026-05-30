"""ChannelQualityMonitor: tracks per-channel throughput and RTT for weighted scheduling."""

import logging
import random
import time
from collections import deque
from typing import Dict, List, Optional, Tuple

from pfrp.constants import MONITOR_WINDOW_SIZE

logger = logging.getLogger(__name__)


class ChannelQualityMonitor:
    """Tracks per-channel throughput and RTT to support weighted channel selection."""

    def __init__(self):
        self._channels: List[str] = []
        # Per-channel deque of (timestamp, bytes_count) samples
        self._samples: Dict[str, deque] = {}
        # Per-channel deque of RTT samples
        self._rtt_samples: Dict[str, deque] = {}
        # Cached weights: channel_id -> float
        self._weights: Dict[str, float] = {}
        # Cached metrics
        self._avg_rtt_ms: float = 0.0
        self._avg_throughput_mbps: float = 0.0
        self._total_throughput_mbps: float = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register_channels(self, channel_ids: List[str]) -> None:
        """Register one or more channel IDs for monitoring."""
        for ch in channel_ids:
            if ch not in self._channels:
                self._channels.append(ch)
            if ch not in self._samples:
                self._samples[ch] = deque(maxlen=MONITOR_WINDOW_SIZE)
            if ch not in self._rtt_samples:
                self._rtt_samples[ch] = deque(maxlen=MONITOR_WINDOW_SIZE)

    def record_sent(self, channel_id: str, bytes_count: int, timestamp: float) -> None:
        """Record that *bytes_count* bytes were sent on *channel_id* at *timestamp*."""
        if channel_id not in self._samples:
            self._samples[channel_id] = deque(maxlen=MONITOR_WINDOW_SIZE)
        self._samples[channel_id].append((timestamp, bytes_count))

    def record_rtt(self, channel_id: str, rtt_ms: float) -> None:
        """Record an RTT sample (in milliseconds) for *channel_id*."""
        if channel_id not in self._rtt_samples:
            self._rtt_samples[channel_id] = deque(maxlen=MONITOR_WINDOW_SIZE)
        self._rtt_samples[channel_id].append(rtt_ms)

    def update_weights(self) -> Dict[str, float]:
        """Recalculate per-channel weights based on observed throughput.

        Weight = channel_throughput / max_throughput
        Throughput = total_bytes / duration for recent samples.

        Returns the new weight dictionary.
        """
        throughputs: Dict[str, float] = {}
        total_bps = 0.0

        for ch in self._channels:
            samples = self._samples.get(ch, deque())
            if not samples:
                throughputs[ch] = 0.0
                continue

            total_bytes = sum(b for _, b in samples)
            timestamps = [t for t, _ in samples]
            duration = max(timestamps) - min(timestamps)
            if duration <= 0:
                # All samples at same instant – treat as a tiny duration
                duration = 1e-9
            bps = (total_bytes * 8) / duration
            throughputs[ch] = bps
            total_bps += bps

        max_tp = max(throughputs.values()) if throughputs else 0.0

        if max_tp == 0.0:
            # Edge case: all channels have 0 throughput -> uniform weights
            self._weights = {ch: 1.0 for ch in self._channels}
        else:
            self._weights = {ch: throughputs[ch] / max_tp for ch in self._channels}

        # Update aggregated metrics
        channel_count = len(self._channels) or 1
        self._avg_throughput_mbps = (total_bps / channel_count) / 1_000_000.0
        self._total_throughput_mbps = total_bps / 1_000_000.0

        # Average RTT across all channels
        all_rtts = []
        for ch in self._channels:
            all_rtts.extend(self._rtt_samples.get(ch, deque()))
        self._avg_rtt_ms = sum(all_rtts) / len(all_rtts) if all_rtts else 0.0

        return dict(self._weights)

    def update_rtt(self) -> float:
        """Recalculate average RTT from recorded samples.

        Returns the current average RTT in milliseconds.
        """
        all_rtts = []
        for ch in self._channels:
            all_rtts.extend(self._rtt_samples.get(ch, deque()))
        self._avg_rtt_ms = sum(all_rtts) / len(all_rtts) if all_rtts else 0.0
        return self._avg_rtt_ms

    def get_weighted_channel(self) -> str:
        """Return a channel ID selected proportionally to its weight.

        Uses cumulative weight / weighted random approach.
        Raises ValueError if no channels are registered.
        """
        if not self._channels:
            raise ValueError("No channels registered")

        # If weights haven't been calculated yet, use uniform
        if not self._weights:
            return random.choice(self._channels)

        # Build cumulative distribution
        candidates = self._channels
        weights = [self._weights.get(ch, 1.0) for ch in candidates]
        total = sum(weights)
        if total == 0.0:
            return random.choice(candidates)

        pick = random.uniform(0.0, total)
        cumulative = 0.0
        for ch, w in zip(candidates, weights):
            cumulative += w
            if pick <= cumulative:
                return ch

        # Fallback (should rarely happen due to floating point)
        return candidates[-1]

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def avg_rtt_ms(self) -> float:
        """Average RTT across all channels (milliseconds)."""
        return self._avg_rtt_ms

    @property
    def avg_throughput_mbps(self) -> float:
        """Average per-channel throughput (Mbps)."""
        return self._avg_throughput_mbps

    @property
    def total_throughput_mbps(self) -> float:
        """Aggregate throughput across all channels (Mbps)."""
        return self._total_throughput_mbps

    @property
    def weights(self) -> Dict[str, float]:
        """Current weight dictionary (channel_id -> weight)."""
        return dict(self._weights)
