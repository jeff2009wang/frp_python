"""Unit tests for FlowClassifier."""

import time

import pytest

from pfrp.flow_classifier import FlowClassifier
from pfrp.channel_monitor import ChannelQualityMonitor
from pfrp.constants import (
    FLOW_RATE_THRESHOLD_MIN,
    FLOW_RATE_THRESHOLD_MAX,
    FLOW_PROMOTION_WINDOW,
    FLOW_BYTES_THRESHOLD_MULTIPLIER,
)


@pytest.fixture
def monitor():
    return ChannelQualityMonitor()


@pytest.fixture
def classifier(monitor):
    return FlowClassifier(monitor)


class TestFlowClassifier:
    """Tests for FlowClassifier."""

    def test_new_stream_starts_single_channel(self, classifier):
        """Test 1: New stream starts in single-channel mode."""
        assert classifier.get_mode("stream1") == classifier.MODE_SINGLE

    def test_stream_promoted_by_rate_threshold(self, classifier):
        """Test 2: Stream promoted to multi-channel by sustained rate."""
        stream_id = "stream1"
        base_ts = time.time()
        # Need rate >= threshold for promotion_window seconds
        # Use a rate well above the max threshold to guarantee promotion
        bytes_per_second = FLOW_RATE_THRESHOLD_MAX + 10 * 1024 * 1024
        for i in range(int(FLOW_PROMOTION_WINDOW) + 2):
            classifier.record_bytes(stream_id, bytes_per_second, base_ts + i)
        assert classifier.get_mode(stream_id) == classifier.MODE_MULTI

    def test_stream_promoted_by_bytes_threshold(self, classifier):
        """Test 3: Stream promoted to multi-channel by total bytes."""
        stream_id = "stream1"
        ts = time.time()
        # Total bytes >= bytes_threshold triggers promotion immediately
        # bytes_threshold = rate_threshold * 10, min rate = 2MB/s -> min bytes = 20MB
        threshold = classifier._get_bytes_threshold()
        classifier.record_bytes(stream_id, threshold + 1, ts)
        assert classifier.get_mode(stream_id) == classifier.MODE_MULTI

    def test_adaptive_thresholds_change_with_bandwidth(self, monitor):
        """Test 4: Adaptive thresholds change with monitor bandwidth."""
        monitor.register_channels(["ch1"])
        base_ts = time.time()
        # Low bandwidth setup: 100 bytes over 1s -> very low throughput
        for i in range(10):
            monitor.record_sent("ch1", 100, base_ts + i * 0.1)
        monitor.update_weights()
        classifier_low = FlowClassifier(monitor)
        threshold_low = classifier_low._get_rate_threshold()

        # High bandwidth setup: 100MB over 1s -> very high throughput
        # Clear old samples first to avoid mixing
        monitor._samples["ch1"].clear()
        for i in range(10):
            monitor.record_sent("ch1", 10 * 1024 * 1024, base_ts + i * 0.1)
        monitor.update_weights()
        classifier_high = FlowClassifier(monitor)
        threshold_high = classifier_high._get_rate_threshold()

        assert threshold_high > threshold_low

    def test_remove_stream_cleans_up_state(self, classifier):
        """Test 5: remove_stream cleans up state."""
        stream_id = "stream1"
        ts = time.time()
        classifier.record_bytes(stream_id, 1000, ts)
        classifier.remove_stream(stream_id)
        # Internal state should be gone immediately after removal
        assert stream_id not in classifier._streams
        # After removal, stream should start fresh in single mode
        assert classifier.get_mode(stream_id) == classifier.MODE_SINGLE

    def test_mode_does_not_change_once_promoted(self, classifier):
        """Test 6: Mode does not change once promoted to multi-channel."""
        stream_id = "stream1"
        ts = time.time()
        threshold = classifier._get_bytes_threshold()
        classifier.record_bytes(stream_id, threshold + 1, ts)
        assert classifier.get_mode(stream_id) == classifier.MODE_MULTI
        # Subsequent calls should remain multi-channel
        classifier.record_bytes(stream_id, 1, ts + 1)
        assert classifier.get_mode(stream_id) == classifier.MODE_MULTI
        classifier.remove_stream(stream_id)
        # After removal and re-adding, it should start single again
        assert classifier.get_mode(stream_id) == classifier.MODE_SINGLE
