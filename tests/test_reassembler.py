"""Unit tests for SequenceReassembler."""

import asyncio
import time

import pytest

from pfrp.reassembler import SequenceReassembler
from pfrp.constants import REASSEMBLER_MAX_BUFFER


@pytest.mark.asyncio
class TestSequenceReassembler:
    """Tests for SequenceReassembler."""

    async def test_in_order_chunks_returned_immediately(self):
        """Test 1: In-order chunks returned immediately."""
        reassembler = SequenceReassembler()
        result = await reassembler.receive(1, b'chunk1')
        assert result == [(1, b'chunk1')]
        result = await reassembler.receive(2, b'chunk2')
        assert result == [(2, b'chunk2')]

    async def test_out_of_order_chunks_buffered_and_returned_when_gap_filled(self):
        """Test 2: Out-of-order chunks buffered, returned when gap filled."""
        reassembler = SequenceReassembler()
        # Receive seq=2 before seq=1
        result = await reassembler.receive(2, b'chunk2')
        assert result == []
        assert reassembler._buffer == {2: b'chunk2'}
        # Now receive seq=1, both should be returned in order
        result = await reassembler.receive(1, b'chunk1')
        assert result == [(1, b'chunk1'), (2, b'chunk2')]
        assert reassembler._buffer == {}
        assert reassembler._next_seq == 3

    async def test_duplicate_chunks_ignored(self):
        """Test 3: Duplicate chunks ignored."""
        reassembler = SequenceReassembler()
        await reassembler.receive(1, b'chunk1')
        await reassembler.receive(2, b'chunk2')
        # Duplicate of already delivered seq
        result = await reassembler.receive(1, b'duplicate1')
        assert result == []
        assert reassembler._next_seq == 3
        # Duplicate of seq already in buffer
        await reassembler.receive(4, b'chunk4')
        result = await reassembler.receive(4, b'duplicate4')
        assert result == []
        assert reassembler._buffer[4] == b'chunk4'

    async def test_buffer_size_limit_enforced(self):
        """Test 4: Buffer size limit enforced."""
        reassembler = SequenceReassembler()
        # Fill buffer beyond limit
        chunk_size = REASSEMBLER_MAX_BUFFER // 2 + 1
        data0 = b'x' * chunk_size
        data1 = b'y' * chunk_size
        # First out-of-order chunk is accepted
        result = await reassembler.receive(2, data0)
        assert result == []
        # Second chunk exceeds limit, should be rejected/ignored
        result = await reassembler.receive(3, data1)
        assert result == []
        assert 3 not in reassembler._buffer
        assert reassembler._buffered_size <= REASSEMBLER_MAX_BUFFER

    async def test_reset_clears_all_state(self):
        """Test 5: Reset clears all state."""
        reassembler = SequenceReassembler()
        await reassembler.receive(1, b'chunk1')
        await reassembler.receive(2, b'chunk2')
        reassembler.reset()
        assert reassembler._next_seq == 1
        assert reassembler._buffer == {}
        assert reassembler._buffered_size == 0
        assert reassembler._last_receive_time == 0.0
        # After reset, seq=1 should be accepted again
        result = await reassembler.receive(1, b'new_chunk1')
        assert result == [(1, b'new_chunk1')]

    async def test_multiple_gaps_handled_correctly(self):
        """Test 6: Multiple gaps handled correctly."""
        reassembler = SequenceReassembler()
        # Receive chunks with gaps: 1, 3, 5
        result = await reassembler.receive(1, b'chunk1')
        assert result == [(1, b'chunk1')]
        result = await reassembler.receive(3, b'chunk3')
        assert result == []
        result = await reassembler.receive(5, b'chunk5')
        assert result == []
        # Fill gap with seq=2
        result = await reassembler.receive(2, b'chunk2')
        assert result == [(2, b'chunk2'), (3, b'chunk3')]
        assert reassembler._next_seq == 4
        # Fill gap with seq=4
        result = await reassembler.receive(4, b'chunk4')
        assert result == [(4, b'chunk4'), (5, b'chunk5')]
        assert reassembler._next_seq == 6
        assert reassembler._buffer == {}

    async def test_large_sequence_numbers(self):
        """Test 7: Large sequence numbers work (uint64 range)."""
        reassembler = SequenceReassembler()
        large_seq = 2**64 - 3
        reassembler._next_seq = large_seq
        result = await reassembler.receive(large_seq, b'chunk_large')
        assert result == [(large_seq, b'chunk_large')]
        result = await reassembler.receive(large_seq + 1, b'chunk_large+1')
        assert result == [(large_seq + 1, b'chunk_large+1')]

    async def test_seq_below_next_ignored(self):
        """Edge case: seq < next_seq is ignored (already delivered)."""
        reassembler = SequenceReassembler()
        await reassembler.receive(1, b'chunk1')
        # seq=0 is below next_seq=2, should be ignored
        result = await reassembler.receive(0, b'old_data')
        assert result == []

    async def test_is_stalled_detects_timeout(self):
        """Stall detection: is_stalled returns True when waiting too long."""
        reassembler = SequenceReassembler()
        # No data received yet, should not be stalled
        assert not reassembler.is_stalled(100)
        # Receive out-of-order chunk to start waiting
        await reassembler.receive(2, b'chunk2')
        # Immediately check with 0 timeout should be stalled
        assert reassembler.is_stalled(0)
        # Check with large timeout should not be stalled
        assert not reassembler.is_stalled(10000)

    async def test_is_stalled_false_when_no_pending(self):
        """Stall detection: is_stalled returns False when no pending chunks."""
        reassembler = SequenceReassembler()
        await reassembler.receive(1, b'chunk1')
        assert not reassembler.is_stalled(0)

    async def test_concurrent_receives_safe(self):
        """Concurrent receives on the same reassembler are safe."""
        reassembler = SequenceReassembler()
        # Send multiple out-of-order chunks concurrently
        coros = [
            reassembler.receive(i, b'x' * 1000)
            for i in range(2, 102)
        ]
        results = await asyncio.gather(*coros)
        # All out-of-order, so all should return empty
        assert all(r == [] for r in results)
        # Buffered size should be exactly what we put in
        assert reassembler._buffered_size == 1000 * 100
        # Now receive seq=1, all should be returned in order
        result = await reassembler.receive(1, b'start')
        assert len(result) == 101
        assert result[0] == (1, b'start')
        assert result[1] == (2, b'x' * 1000)
        assert result[-1] == (101, b'x' * 1000)
