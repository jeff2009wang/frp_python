#!/usr/bin/env python
"""
FRPS Multi-Connection Server - Parallel TCP Transport for Maximum Throughput
"""
import sys
import socket
import struct
import logging
import asyncio
import time
import heapq
from typing import Dict, Optional, Set, List

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('frps_multi')

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


class DataChannel:
    """A single data channel connection with optimized throughput."""
    
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, channel_id: int):
        self.reader = reader
        self.writer = writer
        self.channel_id = channel_id
        self.active = True
        self.bytes_sent = 0
        self.DRAIN_THRESHOLD = 256 * 1024  # Drain every 256KB
        self.DRAIN_INTERVAL = 0.01  # Or every 10ms
    
    async def send(self, stream_id: int, data: bytes):
        """Send TCP data."""
        header = struct.pack('!II', stream_id, len(data))
        self.writer.write(header + data)
        self.bytes_sent += len(data)
        await self.writer.drain()

    async def send_udp(self, payload: bytes):
        """Send UDP data encapsulated in CMD_UDP_DATA."""
        # Use stream_id 0 to indicate control/non-TCP-stream data in the data channel if needed, 
        # but here we follow the framing: [StreamID:4][Length:4][Data:N]
        # For UDP, we can use a special StreamID (e.g. 0xFFFFFFFF) or just wrap it in CMD_UDP_DATA
        # Let's use StreamID 0 for UDP/Special data within the data channel.
        header = struct.pack('!II', 0, len(payload) + 1)
        self.writer.write(header + struct.pack('!B', CMD_UDP_DATA) + payload)
        await self.writer.drain()
    
    async def flush(self):
        await self.writer.drain()
    
    def close(self):
        self.active = False
        self.writer.close()


class FrpsMultiProtocol:
    """Multi-connection FRP Server Protocol."""
    
    def __init__(self):
        self.control_reader: Optional[asyncio.StreamReader] = None
        self.control_writer: Optional[asyncio.StreamWriter] = None
        self.data_channels: List[DataChannel] = []
        self.port_listeners: Dict[int, 'PortListener'] = {}
        self.udp_listeners: Dict[int, 'UDPListener'] = {}
        self.stream_to_user: Dict[int, asyncio.StreamWriter] = {}
        self.stream_to_channel: Dict[int, DataChannel] = {}
        self.stream_ready: Set[int] = set()
        self.next_stream_id = 1
        self._running = True
        self._channel_index = 0
    
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
                # Read control frame: [Cmd:1][Length:4][Data:N]
                header = await self.control_reader.readexactly(5)
                cmd, length = struct.unpack('!BI', header)
                data = await self.control_reader.readexactly(length) if length > 0 else b''
                
                logger.debug(f"Control command received: cmd={cmd}, len={length}")
                
                if cmd == CMD_HEARTBEAT:
                    # Echo back the timestamp for RTT calculation
                    self.control_writer.write(struct.pack('!BI', CMD_HEARTBEAT, len(data)) + data)
                    await self.control_writer.drain()
                elif cmd == CMD_REGISTER_PORT:
                    await self._handle_register_port(data)
                elif cmd == CMD_UNREGISTER_PORT:
                    await self._handle_unregister_port(data)
                elif cmd == CMD_REGISTER_UDP_PORT:
                    await self._handle_register_udp_port(data)
                elif cmd == CMD_UNREGISTER_UDP_PORT:
                    await self._handle_unregister_udp_port(data)
                elif cmd == CMD_UDP_DATA:
                    await self._handle_udp_data(data)
                elif cmd == CMD_CLOSE_STREAM:
                    stream_id = struct.unpack('!I', data)[0]
                    self._cleanup_stream(stream_id)
                elif cmd == CMD_CONNECTION_ACK:
                    stream_id = struct.unpack('!I', data)[0]
                    self.stream_ready.add(stream_id)
                    logger.info(f'Stream {stream_id} ready')
                    
        except asyncio.IncompleteReadError:
            logger.info('Control channel closed')
        except Exception as e:
            logger.error(f'Control error: {e}')
        finally:
            await self._cleanup()
    
    async def handle_data_channel(self, channel: DataChannel):
        """Handle a single data channel (download direction: client → server → user)."""
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
                # Forward to user connection
                elif stream_id in self.stream_to_user:
                    writer = self.stream_to_user[stream_id]
                    t2 = time.time()
                    writer.write(data)
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
    
    async def _handle_register_udp_port(self, data: bytes):
        port = struct.unpack('!I', data)[0]
        if port in self.udp_listeners:
            logger.info(f'UDP Port {port} already registered')
            self.control_writer.write(struct.pack('!BII', CMD_REGISTER_UDP_PORT, 4, port))
            await self.control_writer.drain()
            return

        try:
            listener = UDPListener(port, self)
            transport, protocol = await asyncio.get_running_loop().create_datagram_endpoint(
                lambda: listener, local_addr=('0.0.0.0', port)
            )
            listener.transport = transport
            self.udp_listeners[port] = listener
            
            self.control_writer.write(struct.pack('!BII', CMD_REGISTER_UDP_PORT, 4, port))
            await self.control_writer.drain()
        except Exception as e:
            logger.error(f'Failed to register UDP port {port}: {e}')
            self.control_writer.write(struct.pack('!BII', CMD_REGISTER_UDP_PORT, 4, 0))
            await self.control_writer.drain()

    async def _handle_unregister_udp_port(self, data: bytes):
        port = struct.unpack('!I', data)[0]
        if port in self.udp_listeners:
            self.udp_listeners[port].stop()
            del self.udp_listeners[port]
        self.control_writer.write(struct.pack('!BII', CMD_UNREGISTER_UDP_PORT, 4, port))
        await self.control_writer.drain()

    async def _handle_udp_data(self, data: bytes):
        """Receive UDP data from client and send to original UDP sender."""
        # Frame: [Port:4][IPLen:1][IP:V][RemotePort:2][UDPData:N]
        port = struct.unpack('!I', data[:4])[0]
        ip_len = data[4]
        ip = data[5:5+ip_len].decode()
        remote_port = struct.unpack('!H', data[5+ip_len:7+ip_len])[0]
        udp_content = data[7+ip_len:]
        
        if port in self.udp_listeners:
            self.udp_listeners[port].send_to((ip, remote_port), udp_content)

    def _cleanup_stream(self, stream_id: int):
        """Clean up all resources associated with a stream."""
        if stream_id in self.stream_to_user:
            try:
                self.stream_to_user[stream_id].close()
            except:
                pass
            del self.stream_to_user[stream_id]
        if stream_id in self.stream_to_channel:
            del self.stream_to_channel[stream_id]
        if stream_id in self.stream_ready:
            self.stream_ready.discard(stream_id)
        logger.debug(f'Stream {stream_id} resources cleaned up')
    
    async def _cleanup(self):
        self._running = False
        for listener in list(self.port_listeners.values()):
            await listener.stop()
        self.port_listeners.clear()
        
        for listener in list(self.udp_listeners.values()):
            listener.stop()
        self.udp_listeners.clear()
        
        for channel in self.data_channels:
            channel.close()
        for writer in self.stream_to_user.values():
            try:
                writer.close()
            except:
                pass
        self.stream_to_user.clear()
        self.stream_to_channel.clear()
        self.stream_ready.clear()
    
    async def _handle_register_port(self, data: bytes):
        port = struct.unpack('!I', data)[0]
        
        if port in self.port_listeners:
            logger.info(f'Port {port} already registered')
            self.control_writer.write(struct.pack('!BII', CMD_REGISTER_PORT, 4, port))
            await self.control_writer.drain()
            return
        
        try:
            listener = PortListener(port, self)
            server = await asyncio.start_server(
                listener.handle_client, '0.0.0.0', port,
                limit=16 * 1024 * 1024
            )
            listener.server = server
            self.port_listeners[port] = listener
            asyncio.create_task(server.serve_forever())
            
            self.control_writer.write(struct.pack('!BII', CMD_REGISTER_PORT, 4, port))
            await self.control_writer.drain()
        except Exception as e:
            logger.error(f'Failed to register port {port}: {e}')
            self.control_writer.write(struct.pack('!BII', CMD_REGISTER_PORT, 4, 0))
            await self.control_writer.drain()
    
    async def _handle_unregister_port(self, data: bytes):
        port = struct.unpack('!I', data)[0]
        if port in self.port_listeners:
            await self.port_listeners[port].stop()
            del self.port_listeners[port]
        self.control_writer.write(struct.pack('!BII', CMD_UNREGISTER_PORT, 4, port))
        await self.control_writer.drain()


class PortListener:
    """Listens for user connections."""
    
    def __init__(self, port: int, protocol: FrpsMultiProtocol):
        self.port = port
        self.protocol = protocol
        self.server = None
        self.next_conn_id = 1
    
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        logger.info(f'User from {addr} on port {self.port}')
        
        # Optimize socket
        sock = writer.get_extra_info('socket')
        if sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024 * 1024)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        
        # Increase asyncio high-water mark to match OS buffers
        writer.transport.set_write_buffer_limits(high=16 * 1024 * 1024)
        
        stream_id = self.protocol.next_stream_id
        self.protocol.next_stream_id += 1
        
        # Assign to a data channel
        channel = self.protocol.get_next_channel()
        if not channel:
            logger.error('No data channels available')
            writer.close()
            return
        
        self.protocol.stream_to_user[stream_id] = writer
        self.protocol.stream_to_channel[stream_id] = channel
        
        # Request connection
        conn_id = self.next_conn_id
        self.next_conn_id += 1
        self.protocol.control_writer.write(
            struct.pack('!BIIII', CMD_CONNECTION, 12, stream_id, self.port, conn_id)
        )
        await self.protocol.control_writer.drain()
        
        # Wait for ACK
        for _ in range(50):
            if stream_id in self.protocol.stream_ready:
                break
            await asyncio.sleep(0.1)
        
        if stream_id not in self.protocol.stream_ready:
            logger.warning(f'No ACK for stream {stream_id}')
            writer.close()
            del self.protocol.stream_to_user[stream_id]
            del self.protocol.stream_to_channel[stream_id]
            return
        
        # Forward user data to client via assigned channel
        buffer_size = 512 * 1024
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
            logger.debug(f'User error: {e}')
        finally:
            writer.close()
            logger.info(f'Stream {stream_id} closed')
            # Notify client to close local connection
            if self.protocol.control_writer and not self.protocol.control_writer.is_closing():
                try:
                    self.protocol.control_writer.write(struct.pack('!BII', CMD_CLOSE_STREAM, 4, stream_id))
                    await self.protocol.control_writer.drain()
                except:
                    pass
            
            # Clean up all stream resources
            self.protocol._cleanup_stream(stream_id)
    
    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info(f'Port {self.port} listener stopped')


class UDPListener(asyncio.DatagramProtocol):
    """Listens for UDP packets and forwards them to client."""
    
    def __init__(self, port: int, protocol: FrpsMultiProtocol):
        self.port = port
        self.protocol = protocol
        self.transport = None
    
    def connection_made(self, transport):
        self.transport = transport
        logger.info(f'UDP Listener on port {self.port} started')
    
    def datagram_received(self, data, addr):
        """Receive UDP packet from local source, forward to client via TCP data channel."""
        # logger.debug(f'UDP Packet from {addr} on port {self.port}')
        
        # Map source to a channel (round-robin)
        channel = self.protocol.get_next_channel()
        if not channel:
            return
            
        # Frame: [Cmd:1][Length:4][Port:4][IPLen:1][IP:V][RemotePort:2][UDPData:N]
        ip = addr[0].encode()
        payload = struct.pack('!IB', self.port, len(ip)) + ip + struct.pack('!H', addr[1]) + data
        
        asyncio.create_task(channel.send_udp(payload))
    
    def send_to(self, addr, data):
        if self.transport:
            self.transport.sendto(data, addr)
            
    def stop(self):
        if self.transport:
            self.transport.close()


class FrpsMultiServer:
    """Multi-connection FRP Server."""
    
    def __init__(self, host: str, port: int, num_data_channels: int = 16):
        self.host = host
        self.port = port
        self.num_data_channels = num_data_channels
        self.active_clients: Dict[str, FrpsMultiProtocol] = {} # Key: Peer IP
    
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        
        # Optimize socket
        sock = writer.get_extra_info('socket')
        if sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024 * 1024)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            
        # Increase asyncio high-water mark
        writer.transport.set_write_buffer_limits(high=16 * 1024 * 1024)
        
        # Read connection type
        conn_type_data = await reader.readexactly(1)
        conn_type = struct.unpack('!B', conn_type_data)[0]
        
        if conn_type == CONN_CONTROL:
            logger.info(f'Control connection from {addr}')
            protocol = FrpsMultiProtocol()
            protocol.control_reader = reader
            protocol.control_writer = writer
            
            # Wait for data channels
            logger.info(f'Waiting for {self.num_data_channels} data channels...')
            
            # Store protocol for data channel registration
            client_ip = addr[0]
            self.active_clients[client_ip] = protocol
            
            # Start control handling immediately
            try:
                await protocol.handle_control()
            finally:
                if client_ip in self.active_clients:
                    del self.active_clients[client_ip]
            
        elif conn_type == CONN_DATA:
            # Read channel ID
            channel_id_data = await reader.readexactly(4)
            channel_id = struct.unpack('!I', channel_id_data)[0]
            
            client_ip = addr[0]
            if client_ip in self.active_clients:
                protocol = self.active_clients[client_ip]
                channel = DataChannel(reader, writer, channel_id)
                protocol.data_channels.append(channel)
                logger.info(f'Data channel {channel_id} connected from {addr}')
                
                # Start handling this channel
                await protocol.handle_data_channel(channel)
            else:
                logger.warning(f'Data channel from unknown client: {client_ip}')
                writer.close()
    
    async def start(self):
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port,
            limit=16 * 1024 * 1024
        )
        
        addr = server.sockets[0].getsockname()
        logger.info(f'FRPS Multi-Connection Server on {addr}')
        logger.info(f'Expecting {self.num_data_channels} data channels per client')
        
        async with server:
            await server.serve_forever()


def main():
    if len(sys.argv) < 2:
        print('FRPS Multi-Connection Server v1.0')
        print('Usage: frps_multi <port> [--channels NUM] [--host HOST]')
        print('  --channels NUM  Number of data channels (default: 4)')
        sys.exit(1)
    
    port = int(sys.argv[1])
    host = '0.0.0.0'
    num_channels = 16
    
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == '--host' and i + 1 < len(sys.argv):
            host = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == '--channels' and i + 1 < len(sys.argv):
            num_channels = int(sys.argv[i + 1])
            i += 2
        else:
            i += 1
    
    server = FrpsMultiServer(host, port, num_channels)
    
    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        logger.info('Shutting down...')


if __name__ == '__main__':
    main()
