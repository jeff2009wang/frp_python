"""BatchSender: adaptive batch flushing for high-throughput links."""

import asyncio
import logging

from pfrp.constants import (
    BATCH_DRAIN_THRESHOLD_MIN,
    BATCH_DRAIN_THRESHOLD_MAX,
    BATCH_DRAIN_INTERVAL_MIN,
    BATCH_DRAIN_INTERVAL_MAX,
)

logger = logging.getLogger(__name__)


class BatchSender:
    """Buffers writes and flushes them in batches to reduce await drain() overhead."""

    def __init__(
        self,
        writer,
        drain_threshold: int = BATCH_DRAIN_THRESHOLD_MIN,
        drain_interval_ms: float = BATCH_DRAIN_INTERVAL_MIN,
    ):
        self._writer = writer
        self._buffer = bytearray()
        self._lock = asyncio.Lock()
        self._closed = False
        self._drain_threshold = drain_threshold
        self._drain_interval_ms = drain_interval_ms
        self._flush_task = None
        self._flush_pending = False

    def start(self) -> None:
        """Start the periodic background flush task."""
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = asyncio.create_task(self._periodic_flush())

    @property
    def drain_threshold(self) -> int:
        return self._drain_threshold

    @property
    def drain_interval_ms(self) -> float:
        return self._drain_interval_ms

    def write(self, data: bytes) -> None:
        """Buffer data without blocking."""
        if self._closed:
            raise RuntimeError("BatchSender is closed")
        self._buffer.extend(data)
        # Signal that a flush is needed; periodic task will pick it up
        if len(self._buffer) >= self._drain_threshold:
            self._flush_pending = True

    async def flush(self) -> None:
        """Flush all buffered data to the writer and await drain()."""
        async with self._lock:
            if self._buffer:
                data = self._buffer
                self._buffer = bytearray()
                self._writer.write(data)
                await self._writer.drain()

    async def maybe_flush(self) -> None:
        """Flush only if buffered data exceeds the threshold."""
        if len(self._buffer) >= self._drain_threshold:
            await self.flush()

    async def _periodic_flush(self) -> None:
        """Background task that flushes at regular intervals or when signaled."""
        try:
            while not self._closed:
                await asyncio.sleep(self._drain_interval_ms / 1000.0)
                if self._flush_pending or self._buffer:
                    self._flush_pending = False
                    await self.flush()
        except asyncio.CancelledError:
            # Graceful exit on cancellation
            if self._buffer and not self._closed:
                try:
                    await self.flush()
                except Exception:
                    pass
            raise
        except Exception:
            logger.exception("Periodic flush error")

    def adapt_thresholds(self, rtt_ms: float, throughput_mbps: float) -> None:
        """Adjust drain threshold and interval based on network quality.

        drain_threshold = max(64KB, min(2MB, BDP/4))
        BDP = bandwidth(bps) * RTT(s) / 8
        drain_interval_ms = max(5ms, min(50ms, RTT/2))
        """
        bandwidth_bps = throughput_mbps * 1_000_000.0
        rtt_s = rtt_ms / 1000.0
        bdp = bandwidth_bps * rtt_s / 8.0
        threshold = int(max(BATCH_DRAIN_THRESHOLD_MIN, min(BATCH_DRAIN_THRESHOLD_MAX, bdp / 4.0)))
        interval = max(BATCH_DRAIN_INTERVAL_MIN, min(BATCH_DRAIN_INTERVAL_MAX, rtt_ms / 2.0))
        self._drain_threshold = threshold
        self._drain_interval_ms = interval

    async def close(self) -> None:
        """Flush remaining data and stop the background task."""
        if self._closed:
            return
        self._closed = True
        # Cancel the background task
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        # Final flush under lock
        async with self._lock:
            if self._buffer:
                data = self._buffer
                self._buffer = bytearray()
                self._writer.write(data)
                await self._writer.drain()

    async def __aenter__(self):
        self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
