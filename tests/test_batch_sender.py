"""Unit tests for BatchSender."""

import asyncio
import pytest
import pytest_asyncio

from pfrp.batch_sender import BatchSender
from pfrp.constants import (
    BATCH_DRAIN_THRESHOLD_MIN,
    BATCH_DRAIN_THRESHOLD_MAX,
    BATCH_DRAIN_INTERVAL_MIN,
    BATCH_DRAIN_INTERVAL_MAX,
)


class MockWriter:
    """Mock asyncio.StreamWriter for testing."""

    def __init__(self):
        self._buffer = bytearray()
        self._closed = False
        self._drain_count = 0

    def write(self, data):
        self._buffer.extend(data)

    async def drain(self):
        self._drain_count += 1
        await asyncio.sleep(0)

    def is_closing(self):
        return self._closed

    def close(self):
        self._closed = True

    @property
    def buffer(self):
        return bytes(self._buffer)

    @property
    def drain_count(self):
        return self._drain_count


@pytest.fixture
def mock_writer():
    return MockWriter()


@pytest_asyncio.fixture
async def batch_sender(mock_writer):
    sender = BatchSender(mock_writer)
    sender.start()
    try:
        yield sender
    finally:
        await sender.close()


@pytest.mark.asyncio
async def test_data_buffered_not_immediately_drained(mock_writer, batch_sender):
    """Test 1: Data should be buffered, not immediately drained."""
    batch_sender.write(b"hello")
    await asyncio.sleep(0)
    assert mock_writer.buffer == b""
    assert mock_writer.drain_count == 0


@pytest.mark.asyncio
async def test_drain_triggered_when_threshold_exceeded(mock_writer, batch_sender):
    """Test 2: Drain should trigger when threshold exceeded."""
    data = b"x" * (BATCH_DRAIN_THRESHOLD_MIN + 1)
    batch_sender.write(data)
    await asyncio.sleep(0.01)
    assert mock_writer.buffer == data
    assert mock_writer.drain_count >= 1


@pytest.mark.asyncio
async def test_adaptive_threshold_calculation(batch_sender):
    """Test 3: Adaptive threshold calculation works correctly."""
    # Example: RTT=20ms, throughput=100 Mbps
    # BDP = 100e6 * 0.020 / 8 = 250000 bytes
    # threshold = max(65536, min(2097152, 250000/4=62500)) = 65536
    # interval = max(5, min(50, 20/2=10)) = 10.0
    batch_sender.adapt_thresholds(rtt_ms=20.0, throughput_mbps=100.0)
    assert batch_sender.drain_threshold == BATCH_DRAIN_THRESHOLD_MIN
    assert batch_sender.drain_interval_ms == 10.0

    # Example: RTT=100ms, throughput=50 Mbps
    # BDP = 50e6 * 0.100 / 8 = 625000 bytes
    # threshold = max(65536, min(2097152, 625000/4=156250)) = 156250
    # interval = max(5, min(50, 100/2=50)) = 50.0
    batch_sender.adapt_thresholds(rtt_ms=100.0, throughput_mbps=50.0)
    assert batch_sender.drain_threshold == 156250
    assert batch_sender.drain_interval_ms == BATCH_DRAIN_INTERVAL_MAX

    # Example: RTT=200ms, throughput=200 Mbps
    # BDP = 200e6 * 0.200 / 8 = 5000000 bytes
    # threshold = max(65536, min(2097152, 5000000/4=1250000)) = 1250000
    # interval = max(5, min(50, 200/2=100)) = 50.0
    batch_sender.adapt_thresholds(rtt_ms=200.0, throughput_mbps=200.0)
    assert batch_sender.drain_threshold == 1250000
    assert batch_sender.drain_interval_ms == BATCH_DRAIN_INTERVAL_MAX


@pytest.mark.asyncio
async def test_close_flushes_remaining_data_and_stops_background(mock_writer):
    """Test 4: Close flushes remaining data and stops background task."""
    batch_sender = BatchSender(mock_writer)
    batch_sender.write(b"remaining data")
    await batch_sender.close()
    assert mock_writer.buffer == b"remaining data"
    assert mock_writer.drain_count >= 1
    assert batch_sender._closed is True


@pytest.mark.asyncio
async def test_periodic_flush_works(mock_writer):
    """Test 5: Periodic flush works (use small interval for test speed)."""
    batch_sender = BatchSender(mock_writer, drain_interval_ms=10.0)
    batch_sender.start()
    batch_sender.write(b"periodic")
    # Wait long enough for at least one periodic flush
    await asyncio.sleep(0.05)
    assert mock_writer.buffer == b"periodic"
    assert mock_writer.drain_count >= 1
    await batch_sender.close()
