"""MultiChannelScheduler: splits single stream's data across multiple channels."""

import time
from collections import deque
from typing import Dict, List, Tuple

from pfrp.channel_monitor import ChannelQualityMonitor
from pfrp.constants import CHUNK_SIZE_MIN, CHUNK_SIZE_MAX, CHUNK_SIZE_DIVISOR


class MultiChannelScheduler:
    """Splits a single stream's data into chunks and assigns them to channels.

    Uses adaptive chunk sizing based on BDP (Bandwidth-Delay Product) and
    round-robin channel selection with per-channel in-flight limits to keep
    the reassembler buffer bounded.
    """

    # Safety multiplier for in-flight limit (BDP * multiplier)
    IN_FLIGHT_MULTIPLIER = 2.0

    def __init__(self, monitor: ChannelQualityMonitor):
        self._monitor = monitor
        # stream_id -> next sequence number
        self._seq_counters: Dict[str, int] = {}
        # round-robin index for fair channel selection
        self._rr_index: int = 0
        # Per-channel in-flight tracking: channel_id -> list of (send_time, bytes)
        self._in_flight: Dict[str, List[Tuple[float, int]]] = {}
        # Pending data per stream: stream_id -> bytes
        self._pending_data: Dict[str, bytes] = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _calculate_chunk_size(self) -> int:
        """Calculate adaptive chunk size based on BDP and channel count.

        chunk_size = BDP / num_channels / 4, clamped to [16KB, 256KB].
        """
        num_channels = len(self._monitor._channels) or 1

        # Estimate BDP from monitor throughput and RTT
        # BDP (bytes) = throughput (bytes/s) * RTT (s)
        # throughput_mbps -> bytes/s = Mbps * 1_000_000 / 8
        throughput_bps = self._monitor.total_throughput_mbps * 1_000_000.0 / 8.0
        rtt_s = self._monitor.avg_rtt_ms / 1000.0
        bdp = throughput_bps * rtt_s

        chunk_size = int(bdp / num_channels / CHUNK_SIZE_DIVISOR)
        return max(CHUNK_SIZE_MIN, min(CHUNK_SIZE_MAX, chunk_size))

    def _get_channel_rtt(self, channel_id: str) -> float:
        """Return average RTT for a channel, falling back to global average."""
        samples = self._monitor._rtt_samples.get(channel_id, deque())
        if samples:
            return sum(samples) / len(samples)
        return self._monitor.avg_rtt_ms or 150.0

    def _get_channel_bdp(self, channel_id: str) -> float:
        """Estimate BDP (bytes) for a single channel."""
        num_channels = len(self._monitor._channels) or 1
        total_bps = self._monitor.total_throughput_mbps * 1_000_000.0
        # Approximate per-channel throughput as total / num_channels
        channel_bps = total_bps / num_channels
        rtt_s = self._get_channel_rtt(channel_id) / 1000.0
        return channel_bps * rtt_s / 8.0

    def _cleanup_in_flight(self, channel_id: str) -> None:
        """Remove in-flight entries whose estimated delivery time has passed."""
        now = time.monotonic()
        rtt_ms = self._get_channel_rtt(channel_id)
        max_age = (rtt_ms / 1000.0) * self.IN_FLIGHT_MULTIPLIER
        entries = self._in_flight.get(channel_id, [])
        self._in_flight[channel_id] = [
            (t, b) for t, b in entries if now - t < max_age
        ]

    def _select_channel(self) -> str:
        """Select a channel using round-robin for bounded out-of-order.

        Round-robin ensures each channel gets exactly 1/N of the traffic,
        keeping the reassembler buffer bounded to num_channels * chunk_size.
        """
        channels = self._monitor._channels
        if not channels:
            return None
        ch = channels[self._rr_index % len(channels)]
        self._rr_index += 1
        return ch

    def _get_next_seq(self, stream_id: str) -> int:
        """Return the next sequence number for a stream and increment."""
        seq = self._seq_counters.get(stream_id, 1)
        self._seq_counters[stream_id] = seq + 1
        return seq

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def can_send(self, channel_id: str, bytes_count: int) -> bool:
        """Check whether *channel_id* can accept *bytes_count* more bytes.

        A channel has capacity if its in-flight bytes (tracked since send time)
        plus the new bytes would not exceed BDP * IN_FLIGHT_MULTIPLIER.

        If no throughput data is available yet, returns True to bootstrap.
        """
        self._cleanup_in_flight(channel_id)
        in_flight = sum(b for _, b in self._in_flight.get(channel_id, []))
        bdp = self._get_channel_bdp(channel_id)
        # Bootstrap: if we have no throughput estimate yet, allow sending
        if bdp <= 0:
            return True
        limit = bdp * self.IN_FLIGHT_MULTIPLIER
        return in_flight + bytes_count <= limit

    def record_sent(self, channel_id: str, bytes_count: int) -> None:
        """Record that *bytes_count* bytes were sent on *channel_id*."""
        if channel_id not in self._in_flight:
            self._in_flight[channel_id] = []
        self._in_flight[channel_id].append((time.monotonic(), bytes_count))

    def get_chunks(self, stream_id: str, data: bytes) -> List[Tuple[str, int, bytes]]:
        """Split data into chunks and assign each to a channel.

        Only assigns chunks to channels that have capacity (in-flight < BDP).
        Unsent data is buffered internally and will be included in the next call.

        Args:
            stream_id: Unique identifier for the stream.
            data: The data payload to split.

        Returns:
            List of (channel_id, sequence_number, chunk_data) tuples.
        """
        # Combine with any pending data from previous calls
        pending = self._pending_data.pop(stream_id, b"")
        all_data = pending + data

        chunk_size = self._calculate_chunk_size()
        chunks: List[Tuple[str, int, bytes]] = []

        i = 0
        total_channels = len(self._monitor._channels) or 1
        channels_tried = 0

        while i < len(all_data):
            chunk_data = all_data[i:i + chunk_size]
            channel_id = self._select_channel()
            if channel_id is None:
                break

            if self.can_send(channel_id, len(chunk_data)):
                seq = self._get_next_seq(stream_id)
                chunks.append((channel_id, seq, chunk_data))
                self.record_sent(channel_id, len(chunk_data))
                i += len(chunk_data)
                channels_tried = 0
            else:
                channels_tried += 1
                if channels_tried >= total_channels:
                    # All channels are at capacity
                    break

        # Buffer any unsent data for the next call
        if i < len(all_data):
            self._pending_data[stream_id] = all_data[i:]

        return chunks

    def has_pending_data(self, stream_id: str) -> bool:
        """Return True if *stream_id* has buffered data waiting for capacity."""
        return stream_id in self._pending_data and len(self._pending_data[stream_id]) > 0

    def remove_stream(self, stream_id: str) -> None:
        """Clean up stream sequence tracking and pending data.

        Args:
            stream_id: Unique identifier for the stream.
        """
        self._seq_counters.pop(stream_id, None)
        self._pending_data.pop(stream_id, None)
