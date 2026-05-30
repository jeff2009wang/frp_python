"""Protocol constants and configuration values."""

# Connection types
CONN_CONTROL = 1
CONN_DATA = 2

# Control commands
CMD_HEARTBEAT = 1
CMD_REGISTER_PORT = 2
CMD_UNREGISTER_PORT = 3
CMD_CONNECTION = 4
CMD_CONNECTION_ACK = 5
CMD_REGISTER_UDP_PORT = 6
CMD_UNREGISTER_UDP_PORT = 7
CMD_UDP_DATA = 8
CMD_CLOSE_STREAM = 9
CMD_ENABLE_MULTI_CHANNEL = 10
CMD_MULTI_CHANNEL_ACK = 11
CMD_MULTI_CHANNEL_NACK = 12

# Frame format
FRAME_HEADER_SIZE = 16  # StreamID(4) + Seq(8) + Length(4)
STREAM_ID_SIZE = 4
SEQUENCE_NUMBER_SIZE = 8
LENGTH_SIZE = 4

# Buffer sizes
BUFFER_SIZE_SMALL = 4 * 1024 * 1024      # 4MB
BUFFER_SIZE_LARGE = 16 * 1024 * 1024     # 16MB
BUFFER_SIZE_READ = 512 * 1024            # 512KB
SOCKET_BUFFER_SIZE = 16 * 1024 * 1024    # 16MB

# Batch sender defaults
BATCH_DRAIN_THRESHOLD_MIN = 64 * 1024    # 64KB
BATCH_DRAIN_THRESHOLD_MAX = 2 * 1024 * 1024  # 2MB
BATCH_DRAIN_INTERVAL_MIN = 5.0           # 5ms
BATCH_DRAIN_INTERVAL_MAX = 50.0          # 50ms

# Flow classifier defaults
FLOW_RATE_THRESHOLD_MIN = 2 * 1024 * 1024     # 2MB/s
FLOW_RATE_THRESHOLD_MAX = 50 * 1024 * 1024    # 50MB/s
FLOW_BYTES_THRESHOLD_MULTIPLIER = 10          # bytes = rate * 10s
FLOW_PROMOTION_WINDOW = 2.0                   # 2 seconds

# Multi-channel scheduler
CHUNK_SIZE_MIN = 16 * 1024                    # 16KB
CHUNK_SIZE_MAX = 256 * 1024                   # 256KB
CHUNK_SIZE_DIVISOR = 4                        # BDP / channels / divisor

# Reassembler
REASSEMBLER_TIMEOUT_MS = 500                  # 500ms
REASSEMBLER_MAX_BUFFER = 16 * 1024 * 1024     # 16MB

# Channel monitor
MONITOR_WINDOW_SIZE = 50                      # samples
MONITOR_UPDATE_INTERVAL = 1.0                 # 1 second

# Network quality thresholds
RTT_WEAK_THRESHOLD = 300.0                    # ms
JITTER_WEAK_THRESHOLD = 150.0                 # ms
WEAK_ENTER_SAMPLES = 3
WEAK_EXIT_SAMPLES = 10

# Performance stats
PERF_REPORT_INTERVAL = 2.0                    # seconds
