"""Unit tests for ChannelQualityMonitor."""

import time
import random
from collections import deque

import pytest

from pfrp.channel_monitor import ChannelQualityMonitor
from pfrp.constants import MONITOR_WINDOW_SIZE


@pytest.fixture
def monitor():
    return ChannelQualityMonitor()


class TestRecordSent:
    """Test 1: record_sent increments counters correctly."""

    def test_record_sent_adds_sample(self, monitor):
        ts = time.time()
        monitor.record_sent("ch1", 1000, ts)
        assert "ch1" in monitor._samples
        assert len(monitor._samples["ch1"]) == 1
        assert monitor._samples["ch1"][0] == (ts, 1000)

    def test_record_sent_respects_window_size(self, monitor):
        ts = time.time()
        for i in range(MONITOR_WINDOW_SIZE + 10):
            monitor.record_sent("ch1", 100, ts + i)
        assert len(monitor._samples["ch1"]) == MONITOR_WINDOW_SIZE

    def test_record_sent_multiple_channels(self, monitor):
        ts = time.time()
        monitor.record_sent("ch1", 1000, ts)
        monitor.record_sent("ch2", 2000, ts)
        assert len(monitor._samples["ch1"]) == 1
        assert len(monitor._samples["ch2"]) == 1


class TestUniformWeights:
    """Test 2: With no data, weights are uniform (all 1.0)."""

    def test_weights_uniform_when_no_samples(self, monitor):
        monitor.register_channels(["ch1", "ch2", "ch3"])
        weights = monitor.update_weights()
        assert weights["ch1"] == 1.0
        assert weights["ch2"] == 1.0
        assert weights["ch3"] == 1.0

    def test_weights_uniform_when_zero_bytes(self, monitor):
        monitor.register_channels(["ch1", "ch2"])
        ts = time.time()
        monitor.record_sent("ch1", 0, ts)
        monitor.record_sent("ch2", 0, ts)
        weights = monitor.update_weights()
        assert weights["ch1"] == 1.0
        assert weights["ch2"] == 1.0


class TestHigherThroughputHigherWeight:
    """Test 3: Channel with higher throughput gets higher weight."""

    def test_faster_channel_gets_higher_weight(self, monitor):
        monitor.register_channels(["slow", "fast"])
        base_ts = time.time()
        # slow: 1000 bytes over 1 second
        for i in range(10):
            monitor.record_sent("slow", 100, base_ts + i * 0.1)
        # fast: 10000 bytes over 1 second
        for i in range(10):
            monitor.record_sent("fast", 1000, base_ts + i * 0.1)
        weights = monitor.update_weights()
        assert weights["fast"] > weights["slow"]


class TestWeightedSelection:
    """Test 4: Weighted selection favors faster channels statistically."""

    def test_weighted_selection_favors_faster_channel(self, monitor):
        monitor.register_channels(["slow", "fast"])
        base_ts = time.time()
        for i in range(20):
            monitor.record_sent("slow", 100, base_ts + i * 0.05)
        for i in range(20):
            monitor.record_sent("fast", 1000, base_ts + i * 0.05)
        monitor.update_weights()

        counts = {"slow": 0, "fast": 0}
        for _ in range(1000):
            ch = monitor.get_weighted_channel()
            counts[ch] += 1

        assert counts["fast"] > counts["slow"]

    def test_weighted_selection_returns_registered_channel(self, monitor):
        monitor.register_channels(["ch1", "ch2"])
        ch = monitor.get_weighted_channel()
        assert ch in ("ch1", "ch2")

    def test_weighted_selection_with_no_channels_raises(self, monitor):
        with pytest.raises(ValueError):
            monitor.get_weighted_channel()


class TestUpdateWeights:
    """Test 5: update_weights calculates correct throughput values."""

    def test_throughput_calculation(self, monitor):
        monitor.register_channels(["ch1"])
        base_ts = time.time()
        # Send 10000 bytes over 2 seconds -> 5000 bytes/s
        for i in range(20):
            monitor.record_sent("ch1", 500, base_ts + i * 0.1)
        weights = monitor.update_weights()
        # Only one channel, so weight should be 1.0
        assert weights["ch1"] == 1.0
        # Throughput should be approximately 5000 bytes/s
        assert monitor.avg_throughput_mbps > 0

    def test_total_throughput_aggregation(self, monitor):
        monitor.register_channels(["ch1", "ch2"])
        base_ts = time.time()
        for i in range(10):
            monitor.record_sent("ch1", 1000, base_ts + i * 0.1)
            monitor.record_sent("ch2", 1000, base_ts + i * 0.1)
        monitor.update_weights()
        assert monitor.total_throughput_mbps > 0

    def test_rtt_tracking(self, monitor):
        monitor.register_channels(["ch1"])
        monitor.record_rtt("ch1", 50.0)
        monitor.record_rtt("ch1", 100.0)
        monitor.update_rtt()
        assert monitor.avg_rtt_ms == 75.0

    def test_max_throughput_used_for_normalization(self, monitor):
        monitor.register_channels(["slow", "fast"])
        base_ts = time.time()
        for i in range(10):
            monitor.record_sent("slow", 100, base_ts + i * 0.1)
        for i in range(10):
            monitor.record_sent("fast", 1000, base_ts + i * 0.1)
        weights = monitor.update_weights()
        # fast has max throughput -> weight 1.0
        assert weights["fast"] == 1.0
        # slow has 1/10th throughput -> weight ~0.1
        assert abs(weights["slow"] - 0.1) < 0.01
