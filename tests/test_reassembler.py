"""Unit tests for SequenceReassembler."""

import time

import pytest

from pfrp.reassembler import SequenceReassembler
from pfrp.constants import REASSEMBLER_MAX_BUFFER


class TestSequenceReassembler:
    """Tests for SequenceReassembler."""

    def test_in_order_chunks_returned_immediately(self):
        """Test 1: In-order chunks returned immediately."""
        reassembler = SequenceReassembler()
        result = reassembler.receive(0, b"chunk0")
        assert result == [(0, b"chunk0")]
        result = reassembler.receive(1, b"chunk1")
        assert result == [(1, b"chunk1")]

    def test_out_of_order_chunks_buffered_and_returned_when_gap_filled(self):
        """Test 2: Out-of-order chunks buffered, returned when gap filled."""
        reassembler = SequenceReassembler()
        # Receive seq=1 before seq=0
        result = reassembler.receive(1, b"chunk1")
        assert result == []
        assert reassembler._buffer == {1: b"chunk1"}
        # Now receive seq=0, both should be returned in order
        result = reassembler.receive(0, b"chunk0")
        assert result == [(0, b"chunk0"), (1, b"chunk1")]
        assert reassembler._buffer == {}
        assert reassembler._next_seq == 2

    def test_duplicate_chunks_ignored(self):
        """Test 3: Duplicate chunks ignored."""
        reassembler = SequenceReassembler()
        reassembler.receive(0, b"chunk0")
        reassembler.receive(1, b"chunk1")
        # Duplicate of already delivered seq
        result = reassembler.receive(0, b"duplicate0")
        assert result == []
        assert reassembler._next_seq == 2
        # Duplicate of seq already in buffer
        reassembler.receive(3, b"chunk3")
        result = reassembler.receive(3, b"duplicate3")
        assert result == []
        assert reassembler._buffer[3] == b"chunk3"

    def test_buffer_size_limit_enforced(self):
        """Test 4: Buffer size limit enforced."""
        reassembler = SequenceReassembler()
        # Fill buffer beyond limit
        chunk_size = REASSEMBLER_MAX_BUFFER // 2 + 1
        data0 = b"x" * chunk_size
        data1 = b"y" * chunk_size
        # First out-of-order chunk is accepted
        result = reassembler.receive(1, data0)
        assert result == []
        # Second chunk exceeds limit, should be rejected/ignored
        result = reassembler.receive(2, data1)
        assert result == []
        assert 2 not in reassembler._buffer
        assert reassembler._buffered_size <= REASSEMBLER_MAX_BUFFER

    def test_reset_clears_all_state(self):
        """Test 5: Reset clears all state."""
        reassembler = SequenceReassembler()
        reassembler.receive(1, b"chunk1")
        reassembler.receive(2, b"chunk2")
        reassembler.reset()
        assert reassembler._next_seq == 0
        assert reassembler._buffer == {}
        assert reassembler._buffered_size == 0
        assert reassembler._last_receive_time == 0.0
        # After reset, seq=0 should be accepted again
        result = reassembler.receive(0, b"new_chunk0")
        assert result == [(0, b"new_chunk0")]

    def test_multiple_gaps_handled_correctly(self):
        """Test 6: Multiple gaps handled correctly."""
        reassembler = SequenceReassembler()
        # Receive chunks with gaps: 0, 2, 4
        result = reassembler.receive(0, b"chunk0")
        assert result == [(0, b"chunk0")]
        result = reassembler.receive(2, b"chunk2")
        assert result == []
        result = reassembler.receive(4, b"chunk4")
        assert result == []
        # Fill gap with seq=1
        result = reassembler.receive(1, b"chunk1")
        assert result == [(1, b"chunk1"), (2, b"chunk2")]
        assert reassembler._next_seq == 3
        # Fill gap with seq=3
        result = reassembler.receive(3, b"chunk3")
        assert result == [(3, b"chunk3"), (4, b"chunk4")]
        assert reassembler._next_seq == 5
        assert reassembler._buffer == {}

    def test_large_sequence_numbers(self):
        """Test 7: Large sequence numbers work (uint64 range)."""
        reassembler = SequenceReassembler()
        large_seq = 2**64 - 3
        reassembler._next_seq = large_seq
        result = reassembler.receive(large_seq, b"chunk_large")
        assert result == [(large_seq, b"chunk_large")]
        result = reassembler.receive(large_seq + 1, b"chunk_large+1")
        assert result == [(large_seq + 1, b"chunk_large+1")]

    def test_seq_zero_single_channel_pass_through(self):
        """Edge case: seq == 0 (single-channel mode) should pass through."""
        reassembler = SequenceReassembler()
        result = reassembler.receive(0, b"data")
        assert result == [(0, b"data")]

    def test_is_stalled_detects_timeout(self):
        """Stall detection: is_stalled returns True when waiting too long."""
        reassembler = SequenceReassembler()
        # No data received yet, should not be stalled
        assert not reassembler.is_stalled(100)
        # Receive out-of-order chunk to start waiting
        reassembler.receive(1, b"chunk1")
        # Immediately check with 0 timeout should be stalled
        assert reassembler.is_stalled(0)
        # Check with large timeout should not be stalled
        assert not reassembler.is_stalled(10000)

    def test_is_stalled_false_when_no_pending(self):
        """Stall detection: is_stalled returns False when no pending chunks."""
        reassembler = SequenceReassembler()
        reassembler.receive(0, b"chunk0")
        assert not reassembler.is_stalled(0)
