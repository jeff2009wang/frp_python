"""Unit tests for MultiChannelScheduler."""

import pytest

from pfrp.scheduler import MultiChannelScheduler
from pfrp.channel_monitor import ChannelQualityMonitor
from pfrp.constants import CHUNK_SIZE_MIN, CHUNK_SIZE_MAX


@pytest.fixture
def monitor():
    return ChannelQualityMonitor()


@pytest.fixture
def scheduler(monitor):
    monitor.register_channels(["ch1", "ch2", "ch3"])
    monitor.update_weights()
    return MultiChannelScheduler(monitor)


class TestMultiChannelScheduler:
    """Tests for MultiChannelScheduler."""

    def test_data_split_into_multiple_chunks(self, scheduler):
        """Test 1: Data is split into multiple chunks."""
        stream_id = "stream1"
        data = b"x" * (CHUNK_SIZE_MIN * 5)
        chunks = scheduler.get_chunks(stream_id, data)
        assert len(chunks) > 1
        # All chunks should be tuples of (channel_id, seq, chunk_data)
        for chunk in chunks:
            assert len(chunk) == 3
            channel_id, seq, chunk_data = chunk
            assert isinstance(channel_id, str)
            assert isinstance(seq, int)
            assert isinstance(chunk_data, bytes)

    def test_sequence_numbers_increment_correctly(self, scheduler):
        """Test 2: Sequence numbers increment correctly within a stream."""
        stream_id = "stream1"
        data1 = b"a" * (CHUNK_SIZE_MIN * 3)
        chunks1 = scheduler.get_chunks(stream_id, data1)
        seqs1 = [seq for _, seq, _ in chunks1]
        assert seqs1 == list(range(1, len(chunks1) + 1))

        data2 = b"b" * (CHUNK_SIZE_MIN * 2)
        chunks2 = scheduler.get_chunks(stream_id, data2)
        seqs2 = [seq for _, seq, _ in chunks2]
        assert seqs2 == list(range(len(chunks1) + 1, len(chunks1) + len(chunks2) + 1))

    def test_chunk_size_within_bounds(self, scheduler):
        """Test 3: Each chunk size is within [CHUNK_SIZE_MIN, CHUNK_SIZE_MAX]."""
        stream_id = "stream1"
        # Large data to force multiple chunks
        data = b"y" * (CHUNK_SIZE_MAX * 10)
        chunks = scheduler.get_chunks(stream_id, data)
        for _, _, chunk_data in chunks:
            assert CHUNK_SIZE_MIN <= len(chunk_data) <= CHUNK_SIZE_MAX

    def test_remove_stream_cleans_up_state(self, scheduler):
        """Test 4: remove_stream cleans up stream sequence tracking."""
        stream_id = "stream1"
        data = b"z" * (CHUNK_SIZE_MIN * 2)
        chunks1 = scheduler.get_chunks(stream_id, data)
        first_seq_after = max(seq for _, seq, _ in chunks1)
        scheduler.remove_stream(stream_id)
        chunks2 = scheduler.get_chunks(stream_id, data)
        seqs2 = [seq for _, seq, _ in chunks2]
        # After removal, sequence should restart from 1
        assert seqs2 == list(range(1, len(chunks2) + 1))

    def test_different_streams_have_independent_sequence_counters(self, scheduler):
        """Test 5: Different streams have independent sequence counters."""
        stream_a = "streamA"
        stream_b = "streamB"
        data = b"w" * (CHUNK_SIZE_MIN * 2)

        chunks_a1 = scheduler.get_chunks(stream_a, data)
        seqs_a1 = [seq for _, seq, _ in chunks_a1]

        chunks_b1 = scheduler.get_chunks(stream_b, data)
        seqs_b1 = [seq for _, seq, _ in chunks_b1]

        # Both should start at 1 independently
        assert seqs_a1[0] == 1
        assert seqs_b1[0] == 1

        chunks_a2 = scheduler.get_chunks(stream_a, data)
        seqs_a2 = [seq for _, seq, _ in chunks_a2]
        # Stream A should continue from where it left off
        assert seqs_a2[0] == seqs_a1[-1] + 1
        # Stream B should still be at its own counter
        chunks_b2 = scheduler.get_chunks(stream_b, data)
        seqs_b2 = [seq for _, seq, _ in chunks_b2]
        assert seqs_b2[0] == seqs_b1[-1] + 1
