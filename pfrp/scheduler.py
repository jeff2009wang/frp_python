"""MultiChannelScheduler: splits single stream's data across multiple channels."""

from typing import Dict, List, Tuple

from pfrp.channel_monitor import ChannelQualityMonitor
from pfrp.constants import CHUNK_SIZE_MIN, CHUNK_SIZE_MAX, CHUNK_SIZE_DIVISOR


class MultiChannelScheduler:
    """Splits a single stream's data into chunks and assigns them to channels.

    Uses adaptive chunk sizing based on BDP (Bandwidth-Delay Product) and
    weighted channel selection from the ChannelQualityMonitor.
    """

    def __init__(self, monitor: ChannelQualityMonitor):
        self._monitor = monitor
        # stream_id -> next sequence number
        self._seq_counters: Dict[str, int] = {}
        # round-robin index for fair channel selection
        self._rr_index: int = 0

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

    def get_chunks(self, stream_id: str, data: bytes) -> List[Tuple[str, int, bytes]]:
        """Split data into chunks and assign each to a channel.

        Args:
            stream_id: Unique identifier for the stream.
            data: The data payload to split.

        Returns:
            List of (channel_id, sequence_number, chunk_data) tuples.
        """
        chunk_size = self._calculate_chunk_size()
        chunks: List[Tuple[str, int, bytes]] = []

        for i in range(0, len(data), chunk_size):
            chunk_data = data[i:i + chunk_size]
            channel_id = self._select_channel()
            seq = self._get_next_seq(stream_id)
            chunks.append((channel_id, seq, chunk_data))

        return chunks

    def remove_stream(self, stream_id: str) -> None:
        """Clean up stream sequence tracking.

        Args:
            stream_id: Unique identifier for the stream.
        """
        self._seq_counters.pop(stream_id, None)
