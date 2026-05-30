#!/usr/bin/env python
"""
FRPC Multi-Connection Client - Parallel TCP Transport for Maximum Throughput
"""
import sys
import os
import socket
import struct
import logging
import asyncio
import time
import threading
from typing import Dict, Optional, Set, List
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('frpc_multi')

# Connection types
CONN_CONTROL = 1
CONN_DATA = 2

# Commands
CMD_HEARTBEAT = 1
CMD_REGISTER_PORT = 2
CMD_UNREGISTER_PORT = 3
CMD_CONNECTION = 4
CMD_CONNECTION_ACK = 5
CMD_REGISTER_UDP_PORT = 6
CMD_UNREGISTER_UDP_PORT = 7
CMD_UDP_DATA = 8
CMD_CLOSE_STREAM = 9

# Frame: [StreamID:4][Length:4][Data:N]
FRAME_HEADER_SIZE = 8


class PerformanceStats:
    """Track performance metrics for debugging."""
    
    def __init__(self):
        self.bytes_sent = 0
        self.bytes_recv = 0
        self.packets_sent = 0
        self.packets_recv = 0
        self.last_report_time = time.time()
        self.report_interval = 2.0
        
        self.time_reading = 0.0
        self.time_sending = 0.0
        self.read_count = 0
        self.send_count = 0
    
    def add_recv(self, size: int):
        self.bytes_recv += size
        self.packets_recv += 1
    
    def add_sent(self, size: int):
        self.bytes_sent += size
        self.packets_sent += 1
    
    def add_read_time(self, elapsed: float):
        self.time_reading += elapsed
        self.read_count += 1
    
    def add_send_time(self, elapsed: float):
        self.time_sending += elapsed
        self.send_count += 1
    
    def maybe_report(self):
        now = time.time()
        if now - self.last_report_time >= self.report_interval:
            duration = now - self.last_report_time
            recv_rate = self.bytes_recv / (1024 * 1024) / duration
            sent_rate = self.bytes_sent / (1024 * 1024) / duration
            avg_read = (self.time_reading * 1000 / self.read_count) if self.read_count > 0 else 0
            avg_send = (self.time_sending * 1000 / self.send_count) if self.send_count > 0 else 0
            
            stat_line = (f'[PERF] Recv: {recv_rate:6.2f} MB/s | Sent: {sent_rate:6.2f} MB/s | '
                         f'Wait/Proc: {avg_read:7.2f}ms/{avg_send:5.2f}ms')
            sys.stdout.write(f"\r{time.strftime('%H:%M:%S')} {stat_line}")
            sys.stdout.flush()
            
            self.bytes_sent = 0
            self.bytes_recv = 0
            self.packets_sent = 0
            self.packets_recv = 0
            self.time_reading = 0.0
            self.time_sending = 0.0
            self.read_count = 0
            self.send_count = 0
            self.last_report_time = now


perf_stats = PerformanceStats()


class NetworkMonitor:
    """Monitors network quality via Heartbeat RTT."""
    
    def __init__(self):
        self.rtt_history: List[float] = []
        self.last_rtt = 0.0
        self.avg_rtt = 0.0
        self.jitter = 0.0
        self.is_weak = False
        self.weak_count = 0
        self.strong_count = 0
        
    def add_sample(self, rtt_ms: float):
        self.last_rtt = rtt_ms
        self.rtt_history.append(rtt_ms)
        if len(self.rtt_history) > 10:
            self.rtt_history.pop(0)
            
        # Calculate Stats
        self.avg_rtt = sum(self.rtt_history) / len(self.rtt_history)
        
        # Calculate Jitter (Standard Deviation)
        if len(self.rtt_history) > 1:
            variance = sum((x - self.avg_rtt) ** 2 for x in self.rtt_history) / len(self.rtt_history)
            self.jitter = variance ** 0.5
            
            
        # Adaptive Weak Network Detection
        # Relaxed jitter threshold to 150ms to be less sensitive
        is_currently_weak = self.avg_rtt > 300 or self.jitter > 150
        
        if is_currently_weak:
            self.weak_count += 1
            self.strong_count = 0
        else:
            self.strong_count += 1
            self.weak_count = 0
            
        # Hysteresis: Enter weak mode fast (3 samples), exit slow (10 samples)
        if not self.is_weak and self.weak_count >= 3:
            self.is_weak = True
            logger.warning(f'[ADAPTIVE] Entering WEAK NETWORK mode (RTT={self.avg_rtt:.1f}ms, Jitter={self.jitter:.1f}ms)')
            return True # State changed
        elif self.is_weak and self.strong_count >= 10:
            self.is_weak = False
            logger.info(f'[ADAPTIVE] Entering STRONG NETWORK mode (RTT={self.avg_rtt:.1f}ms, Jitter={self.jitter:.1f}ms)')
            return True # State changed
            
        # Log status
        if self.is_weak:
            logger.warning(f'[WEAK] AvgRTT: {self.avg_rtt:.1f}ms | Jitter: {self.jitter:.1f}ms | Last: {rtt_ms:.1f}ms')
        else:
            logger.info(f'[NET] AvgRTT: {self.avg_rtt:.1f}ms | Jitter: {self.jitter:.1f}ms')
            
        return False # State did not change

net_monitor = NetworkMonitor()


class DataChannel:
    """A single data channel connection with optimized throughput."""
    
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, channel_id: int):
        self.reader = reader
        self.writer = writer
        self.channel_id = channel_id
        self.active = True
    
    async def send(self, stream_id: int, data: bytes):
        """Send TCP data."""
        header = struct.pack('!II', stream_id, len(data))
        self.writer.write(header + data)
        await self.writer.drain()

    async def send_udp(self, payload: bytes):
        """Send UDP data encapsulated in CMD_UDP_DATA."""
        header = struct.pack('!II', 0, len(payload) + 1)
        self.writer.write(header + struct.pack('!B', CMD_UDP_DATA) + payload)
        await self.writer.drain()
    
    async def flush(self):
        await self.writer.drain()
        
    def set_buffer_size(self, size_mb: int):
        """Dynamically adjust write buffer limits."""
        limit = size_mb * 1024 * 1024
        self.writer.transport.set_write_buffer_limits(high=limit)
    
    def close(self):
        self.active = False
        self.writer.close()


class UDPForwarder(asyncio.DatagramProtocol):
    """Handles local UDP target communication."""
    def __init__(self, port, remote_ip, remote_port, protocol):
        self.port = port
        self.remote_ip = remote_ip
        self.remote_port = remote_port
        self.protocol = protocol
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        """Reply from local target, send back to server."""
        channel = self.protocol.get_next_channel()
        if not channel:
            return
            
        ip_bytes = self.remote_ip.encode()
        payload = struct.pack('!IB', self.port, len(ip_bytes)) + ip_bytes + struct.pack('!H', self.remote_port) + data
        asyncio.create_task(channel.send_udp(payload))


class FrpcMultiProtocol:
    """Multi-connection FRP Client Protocol."""
    
    def __init__(self, target_host: str = '127.0.0.1'):
        self.target_host = target_host
        self.control_reader: Optional[asyncio.StreamReader] = None
        self.control_writer: Optional[asyncio.StreamWriter] = None
        self.data_channels: List[DataChannel] = []
        self.registered_ports: Set[int] = set()
        self.active_streams: Dict[int, asyncio.StreamWriter] = {}
        self.stream_to_channel: Dict[int, DataChannel] = {}
        self.udp_clients: Dict = {}
        self._running = True
        self._channel_index = 0
        self._tasks: Set[asyncio.Task] = set()
    
    def get_next_channel(self) -> DataChannel:
        """Round-robin channel selection."""
        if not self.data_channels:
            return None
        channel = self.data_channels[self._channel_index % len(self.data_channels)]
        self._channel_index += 1
        return channel
    
    async def handle_control(self):
        """Handle control channel messages."""
        logger.info("Control loop started")
        try:
            while self._running:
                header = await self.control_reader.readexactly(5)
                cmd, length = struct.unpack('!BI', header)
                data = await self.control_reader.readexactly(length) if length > 0 else b''
                
                logger.debug(f"Control command received: cmd={cmd}, len={length}")
                
                if cmd == CMD_HEARTBEAT:
                    if len(data) == 8:
                        sent_time = struct.unpack('!d', data)[0]
                        rtt_ms = (time.time() - sent_time) * 1000
                        if net_monitor.add_sample(rtt_ms):
                            # State changed! Adjust buffers
                            self.adjust_network_mode(net_monitor.is_weak)
                    else:
                        logger.debug('Heartbeat ack')
                elif cmd == CMD_REGISTER_PORT:
                    port = struct.unpack('!I', data)[0]
                    if port > 0:
                        logger.info(f'Port {port} registered')
                    else:
                        logger.warning('Port registration failed')
                elif cmd == CMD_UNREGISTER_PORT:
                    port = struct.unpack('!I', data)[0]
                    logger.info(f'Port {port} unregistered')
                elif cmd == CMD_CONNECTION:
                    stream_id, port, conn_id = struct.unpack('!III', data)
                    await self._handle_connection(stream_id, port, conn_id)
                elif cmd == CMD_CLOSE_STREAM:
                    stream_id = struct.unpack('!I', data)[0]
                    self._cleanup_stream(stream_id)
                elif cmd == CMD_REGISTER_UDP_PORT:
                    port = struct.unpack('!I', data)[0]
                    if port > 0:
                        logger.info(f'UDP Port {port} registered')
                    else:
                        logger.warning('UDP Port registration failed')
                elif cmd == CMD_UNREGISTER_UDP_PORT:
                    port = struct.unpack('!I', data)[0]
                    logger.info(f'UDP Port {port} unregistered')
                    
        except asyncio.IncompleteReadError:
            logger.info('Control channel closed')
        except Exception as e:
            logger.error(f'Control error: {e}')
        finally:
            self._cleanup()
    
    async def handle_data_channel(self, channel: DataChannel):
        """Handle incoming data on a channel (download: server → client → local)."""
        try:
            while channel.active and self._running:
                t0 = time.time()
                header = await channel.reader.readexactly(FRAME_HEADER_SIZE)
                stream_id, length = struct.unpack('!II', header)
                data = await channel.reader.readexactly(length) if length > 0 else b''
                t1 = time.time()
                
                perf_stats.add_read_time(t1 - t0)
                perf_stats.add_recv(len(data))
                
                if stream_id == 0:
                    # Special data (UDP or Control)
                    if data.startswith(struct.pack('!B', CMD_UDP_DATA)):
                        await self._handle_udp_data(data[1:])
                elif stream_id in self.active_streams:
                    writer = self.active_streams[stream_id]
                    t2 = time.time()
                    writer.write(data)
                    await writer.drain()
                    t3 = time.time()
                    
                    perf_stats.add_send_time(t3 - t2)
                    perf_stats.add_sent(len(data))
                    perf_stats.maybe_report()
                    
        except asyncio.IncompleteReadError:
            logger.info(f'Data channel {channel.channel_id} closed')
        except Exception as e:
            logger.debug(f'Data channel error: {e}')
        finally:
            channel.close()

    async def _handle_udp_data(self, data: bytes):
        """Receive UDP data from server and send to local target."""
        # Frame: [Port:4][IPLen:1][IP:V][RemotePort:2][UDPData:N]
        if len(data) < 7:
            logger.warning(f'UDP data too short: {len(data)} bytes')
            return
        port = struct.unpack('!I', data[:4])[0]
        ip_len = data[4]
        if len(data) < 7 + ip_len:
            logger.warning(f'UDP data truncated: expected {7 + ip_len} bytes, got {len(data)}')
            return
        ip = data[5:5+ip_len].decode()
        remote_port = struct.unpack('!H', data[5+ip_len:7+ip_len])[0]
        udp_content = data[7+ip_len:]

        # Limit UDP sessions to prevent memory exhaustion
        if len(self.udp_clients) >= 1000:
            logger.warning('UDP client limit reached, dropping packet')
            return

        key = (port, ip, remote_port)
        if key not in self.udp_clients:
            try:
                # Create a local UDP client for this session
                loop = asyncio.get_running_loop()
                transport, protocol = await loop.create_datagram_endpoint(
                    lambda: UDPForwarder(port, ip, remote_port, self),
                    remote_addr=(self.target_host, port)
                )
                self.udp_clients[key] = {
                    'transport': transport,
                    'last_activity': time.time()
                }
                logger.info(f"Created new UDP session for {ip}:{remote_port} -> local:{port}")
            except Exception as e:
                logger.error(f'Failed to create local UDP client: {e}')
                return

        entry = self.udp_clients[key]
        try:
            entry['transport'].sendto(udp_content)
        except Exception as e:
            logger.error(f'Failed to send UDP data: {e}')
        entry['last_activity'] = time.time()
    
    def adjust_network_mode(self, is_weak: bool):
        """Adjust buffer sizes based on network mode."""
        # 1MB is too small for 3.5Mbps, 4MB covers ~10s of data
        size = 4 if is_weak else 16
        logger.info(f'[ADAPTIVE] Tuning TCP buffers to {size}MB (Weak={is_weak})')
        
        # Tune Data Channels
        for channel in self.data_channels:
            if channel.active:
                channel.set_buffer_size(size)
                
        # Tune Active User Streams
        for writer in self.active_streams.values():
            if not writer.is_closing():
                writer.transport.set_write_buffer_limits(high=size * 1024 * 1024)
    
    def _cleanup_stream(self, stream_id: int):
        """Clean up all resources associated with a stream."""
        if stream_id in self.active_streams:
            try:
                self.active_streams[stream_id].close()
            except Exception:
                pass
            del self.active_streams[stream_id]
        if stream_id in self.stream_to_channel:
            del self.stream_to_channel[stream_id]
        logger.debug(f'Stream {stream_id} resources cleaned up')
    
    def _cleanup(self):
        self._running = False
        for channel in self.data_channels:
            channel.close()
        for writer in list(self.active_streams.values()):
            try:
                writer.close()
            except Exception:
                pass
        for entry in self.udp_clients.values():
            try:
                entry['transport'].close()
            except Exception:
                pass
        self.active_streams.clear()
        self.stream_to_channel.clear()
        self.udp_clients.clear()
    
    async def register_port(self, port: int):
        """Register a TCP port."""
        self.control_writer.write(struct.pack('!BII', CMD_REGISTER_PORT, 4, port))
        await self.control_writer.drain()
        self.registered_ports.add(port)
    
    async def register_udp_port(self, port: int):
        """Register a UDP port."""
        self.control_writer.write(struct.pack('!BII', CMD_REGISTER_UDP_PORT, 4, port))
        await self.control_writer.drain()

    async def unregister_port(self, port: int):
        """Unregister a TCP port."""
        self.control_writer.write(struct.pack('!BII', CMD_UNREGISTER_PORT, 4, port))
        await self.control_writer.drain()
        self.registered_ports.discard(port)
    
    async def unregister_udp_port(self, port: int):
        """Unregister a UDP port."""
        self.control_writer.write(struct.pack('!BII', CMD_UNREGISTER_UDP_PORT, 4, port))
        await self.control_writer.drain()
    
    async def _handle_connection(self, stream_id: int, port: int, conn_id: int):
        """Handle connection request from server."""
        logger.info(f'Connection request: stream={stream_id}, port={port}')
        
        try:
            reader, writer = await asyncio.open_connection(self.target_host, port)
            
            sock = writer.get_extra_info('socket')
            if sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16 * 1024 * 1024)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024 * 1024)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            
            # Assign channel
            channel = self.get_next_channel()
            if not channel:
                logger.error('No data channels')
                writer.close()
                return
            
            # Increase buffer for user connection
            writer.transport.set_write_buffer_limits(high=16 * 1024 * 1024)
            
            self.active_streams[stream_id] = writer
            self.stream_to_channel[stream_id] = channel
            
            # Send ACK
            self.control_writer.write(struct.pack('!BII', CMD_CONNECTION_ACK, 4, stream_id))
            await self.control_writer.drain()
            
            # Forward local data to server
            self._create_task(self._forward_to_server(reader, stream_id, channel), name=f'forward_{stream_id}')
            
        except Exception as e:
            logger.error(f'Connection failed for port {port}: {e}')
    
    async def _forward_to_server(self, reader: asyncio.StreamReader, stream_id: int, channel: DataChannel):
        """Forward local target data to server."""
        buffer_size = 512 * 1024  # 512KB read buffer
        try:
            while not reader.at_eof():
                t0 = time.time()
                data = await reader.read(buffer_size)
                t1 = time.time()
                
                if not data:
                    break
                
                perf_stats.add_read_time(t1 - t0)
                perf_stats.add_recv(len(data))
                
                t2 = time.time()
                await channel.send(stream_id, data)
                t3 = time.time()
                
                perf_stats.add_send_time(t3 - t2)
                perf_stats.add_sent(len(data))
                perf_stats.maybe_report()
                
        except Exception as e:
            logger.debug(f'Forward error: {e}')
        finally:
            logger.info(f'Stream {stream_id} closed')
            
            # Notify server to close user connection
            if self.control_writer and not self.control_writer.is_closing():
                try:
                    self.control_writer.write(struct.pack('!BII', CMD_CLOSE_STREAM, 4, stream_id))
                    # Note: We don't await drain here to avoid blocking cleanup if control is congested
                except Exception:
                    pass

            # Clean up all stream resources
            self._cleanup_stream(stream_id)


class PortScanner:
    """Port scanner."""
    
    def __init__(self, scan_interval: int = 20, custom_ports: Optional[List[int]] = None, max_workers: int = 50):
        self.scan_interval = scan_interval
        self.custom_ports = custom_ports or []
        self.max_workers = max_workers
        self.active_ports: Set[int] = set()
        self.running = False
        self.lock = threading.Lock()
        self.on_port_change = None
        self.scan_cursor = 1
        self.batch_size = 20000
        self.full_scan_interval = 600
        self.last_full_scan_time = 0
    
    def check_port(self, host: str, port: int, timeout: float = 0.3) -> bool:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(timeout)
            result = sock.connect_ex((host, port))
            return result == 0
        except Exception:
            return False
        finally:
            sock.close()
    
    def scan_ports_fast(self, host: str = '127.0.0.1', ports: Optional[List[int]] = None) -> List[int]:
        if ports is None:
            ports = self.custom_ports if self.custom_ports else list(range(1, 65536))
        active = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.check_port, host, p): p for p in ports}
            for future in as_completed(futures):
                if future.result():
                    active.append(futures[future])
        return sorted(active)
    
    def scan(self, host: str = '127.0.0.1') -> Dict:
        current = time.time()
        if (current - self.last_full_scan_time) >= self.full_scan_interval:
            self.last_full_scan_time = current
            return self.scan_full(host)
        return self.scan_incremental(host)
    
    def scan_incremental(self, host: str = '127.0.0.1') -> Dict:
        start = self.scan_cursor
        end = min(start + self.batch_size, 65536)
        ports = list(range(start, end))
        if not ports:
            self.scan_cursor = 1
            ports = list(range(1, min(self.batch_size + 1, 65536)))
        batch_active = set(self.scan_ports_fast(host, ports))
        with self.lock:
            new_ports = batch_active - self.active_ports
            closed = set()
            for p in self.active_ports:
                if start <= p < end and p not in batch_active:
                    closed.add(p)
            for p in new_ports:
                if self.on_port_change:
                    self.on_port_change('new', p)
            for p in closed:
                if self.on_port_change:
                    self.on_port_change('closed', p)
            self.active_ports.update(batch_active)
            self.active_ports -= closed
            self.scan_cursor = end
        return {'new': sorted(new_ports), 'closed': sorted(closed)}
    
    def scan_full(self, host: str = '127.0.0.1') -> Dict:
        current = set(self.scan_ports_fast(host))
        with self.lock:
            new_ports = current - self.active_ports
            closed = self.active_ports - current
            for p in new_ports:
                if self.on_port_change:
                    self.on_port_change('new', p)
            for p in closed:
                if self.on_port_change:
                    self.on_port_change('closed', p)
            self.active_ports = current
        return {'new': sorted(new_ports), 'closed': sorted(closed)}
    
    def start_continuous_scan(self, host: str = '127.0.0.1'):
        self.running = True
        while self.running:
            try:
                self.scan(host)
            except Exception as e:
                logger.error(f'Scan error: {e}')
            time.sleep(self.scan_interval)
    
    def stop(self):
        self.running = False


class FrpcMultiClient:
    """Multi-connection FRP Client."""

    def __init__(self, server_host: str, server_port: int, target_host: str = '127.0.0.1',
                 num_channels: int = 12, scan_interval: int = 20, ports: Optional[List[int]] = None,
                 udp_ports: Optional[List[int]] = None):
        self.server_host = server_host
        self.server_port = server_port
        self.target_host = target_host
        self.num_channels = num_channels
        self.udp_ports = udp_ports or []
        self.running = True
        self.client_id = struct.unpack('!I', os.urandom(4))[0]

        self.protocol: Optional[FrpcMultiProtocol] = None
        self.port_change_queue: List[tuple] = []
        self.port_change_lock = threading.Lock()

        self.scanner = PortScanner(scan_interval, ports, 50)
        self.scanner.on_port_change = self.on_port_change
    
    async def _create_optimized_socket(self, host, port):
        """Create a pre-configured socket with large buffers to enable Window Scaling."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16 * 1024 * 1024)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024 * 1024)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setblocking(False)
        
        loop = asyncio.get_running_loop()
        await loop.sock_connect(sock, (host, port))
        return sock

    def _create_task(self, coro, name: str = None) -> asyncio.Task:
        """Create a tracked asyncio task."""
        task = asyncio.create_task(coro, name=name)
        self.protocol._tasks.add(task)
        task.add_done_callback(lambda t: self.protocol._tasks.discard(t))
        return task

    async def connect(self):
        logger.info(f'Connecting to {self.server_host}:{self.server_port}')

        # Create protocol
        self.protocol = FrpcMultiProtocol(self.target_host)

        # Connect control channel
        sock = await self._create_optimized_socket(self.server_host, self.server_port)
        reader, writer = await asyncio.open_connection(sock=sock)
        # self._optimize_socket(writer) # Already optimized via pre-creation
        writer.transport.set_write_buffer_limits(high=16 * 1024 * 1024)

        writer.write(struct.pack('!BI', CONN_CONTROL, self.client_id))
        await writer.drain()

        self.protocol.control_reader = reader
        self.protocol.control_writer = writer
        logger.info('Control channel connected')

        # Connect data channels
        for i in range(self.num_channels):
            sock = await self._create_optimized_socket(self.server_host, self.server_port)
            dr, dw = await asyncio.open_connection(sock=sock)
            # self._optimize_socket(dw) # Already optimized
            dw.transport.set_write_buffer_limits(high=16 * 1024 * 1024)

            dw.write(struct.pack('!BII', CONN_DATA, self.client_id, i))
            # No drain needed for small packet

            channel = DataChannel(dr, dw, i)
            self.protocol.data_channels.append(channel)
            self._create_task(self.protocol.handle_data_channel(channel), name=f'data_channel_{i}')
            logger.info(f'Data channel {i} connected')

        logger.info(f'All {self.num_channels} data channels ready')

        # Start tasks
        self._create_task(self._heartbeat_loop(), name='heartbeat')
        self._create_task(self._port_monitor_loop(), name='port_monitor')
        self._create_task(self._process_port_changes(), name='port_changes')
        self._create_task(self._udp_cleanup_loop(), name='udp_cleanup')

        # Initial UDP registration
        if self.udp_ports:
            logger.info(f'Registering UDP ports: {", ".join(map(str, self.udp_ports))}')
            for port in self.udp_ports:
                await self.protocol.register_udp_port(port)

        await self.protocol.handle_control()
    
    def _optimize_socket(self, writer):
        sock = writer.get_extra_info('socket')
        if sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16 * 1024 * 1024)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024 * 1024)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        
        # Increase asyncio high-water mark
        writer.transport.set_write_buffer_limits(high=16 * 1024 * 1024)
    
    async def _heartbeat_loop(self):
        while self.running and self.protocol:
            try:
                if not self.protocol or not self.protocol.control_writer or self.protocol.control_writer.is_closing():
                    break
                # Send heartbeat with timestamp
                payload = struct.pack('!d', time.time())
                self.protocol.control_writer.write(struct.pack('!BI', CMD_HEARTBEAT, len(payload)) + payload)
                await self.protocol.control_writer.drain()
                await asyncio.sleep(2) # Faster heartbeat for monitoring
            except Exception:
                break
    
    async def _port_monitor_loop(self):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, self.scanner.start_continuous_scan, self.target_host
        )
    
    async def _process_port_changes(self):
        while self.running:
            with self.port_change_lock:
                if self.port_change_queue:
                    batch = self.port_change_queue[:10]
                    self.port_change_queue = self.port_change_queue[10:]
                    
                    tasks = []
                    new_tcp = [port for ct, port in batch if ct == 'new']
                    closed_tcp = [port for ct, port in batch if ct == 'closed']
                    
                    if new_tcp:
                        logger.info(f'Adding TCP ports: {", ".join(map(str, new_tcp))}')
                    if closed_tcp:
                        logger.info(f'Closing TCP ports: {", ".join(map(str, closed_tcp))}')

                    for change_type, port in batch:
                        if change_type == 'new':
                            tasks.append(self.protocol.register_port(port))
                        elif change_type == 'closed':
                            tasks.append(self.protocol.unregister_port(port))
                    
                    if tasks:
                        # Register/Unregister in parallel for multi-port optimization
                        await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(0.05)
    
    async def _udp_cleanup_loop(self):
        """Periodically clean up idle UDP sessions."""
        while self.running and self.protocol:
            try:
                await asyncio.sleep(30)
                if not hasattr(self.protocol, 'udp_clients'):
                    continue
                
                now = time.time()
                timeout = 60 # 60 seconds idle timeout
                to_delete = []
                
                for key, entry in self.protocol.udp_clients.items():
                    if now - entry['last_activity'] > timeout:
                        entry['transport'].close()
                        to_delete.append(key)
                        logger.info(f"Cleaned up idle UDP session for {key[1]}:{key[2]}")
                
                for key in to_delete:
                    del self.protocol.udp_clients[key]
            except Exception as e:
                logger.error(f"UDP cleanup error: {e}")
                await asyncio.sleep(5)

    def on_port_change(self, change_type: str, port: int):
        with self.port_change_lock:
            self.port_change_queue.append((change_type, port))
    
    def stop(self):
        self.running = False
        self.scanner.stop()


def main():
    if len(sys.argv) < 3:
        print('FRPC Multi-Connection Client v1.0')
        print('Usage: frpc_multi <server_host> <server_port> [options]')
        print('  --channels NUM    Data channels (default: 4)')
        print('  --target HOST     Target host (default: 127.0.0.1)')
        print('  --interval SECS   Scan interval (default: 20)')
        print('  --ports PORTS     Ports to monitor (comma-separated)')
        sys.exit(1)
    
    server_host = sys.argv[1]
    server_port = int(sys.argv[2])
    
    target_host = '127.0.0.1'
    num_channels = 16
    scan_interval = 20
    ports = None
    udp_ports = []
    
    i = 3
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '--channels' and i + 1 < len(sys.argv):
            num_channels = int(sys.argv[i + 1])
            i += 2
        elif arg == '--target' and i + 1 < len(sys.argv):
            target_host = sys.argv[i + 1]
            i += 2
        elif arg == '--interval' and i + 1 < len(sys.argv):
            scan_interval = int(sys.argv[i + 1])
            i += 2
        elif arg == '--ports' and i + 1 < len(sys.argv):
            ports = [int(p) for p in sys.argv[i + 1].split(',')]
            i += 2
        elif arg == '--udp-ports' and i + 1 < len(sys.argv):
            udp_ports = [int(p) for p in sys.argv[i + 1].split(',')]
            i += 2
        else:
            i += 1
    
    client = FrpcMultiClient(server_host, server_port, target_host, num_channels, scan_interval, ports, udp_ports)
    
    print(f'FRPC Multi-Connection Client v1.0')
    print(f'Server: {server_host}:{server_port}')
    print(f'Target: {target_host}')
    print(f'Data channels: {num_channels}')
    print()
    
    try:
        asyncio.run(client.connect())
    except KeyboardInterrupt:
        logger.info('Shutting down...')
        client.stop()


if __name__ == '__main__':
    main()
