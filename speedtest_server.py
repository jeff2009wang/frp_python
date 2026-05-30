#!/usr/bin/env python3
"""Simple HTTP speed test server."""

import http.server
import socketserver
import os
import sys

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
DOWNLOAD_MB = 8
UPLOAD_MB = 4

HTML_PAGE = """
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Speed Test</title>
<style>
  body { font-family: Arial, sans-serif; max-width: 800px; margin: 40px auto; padding: 20px; background: #1a1a2e; color: #eee; }
  h1 { text-align: center; color: #00d4aa; }
  .card { background: #16213e; border-radius: 12px; padding: 24px; margin: 16px 0; }
  .metric { display: flex; justify-content: space-between; align-items: center; padding: 12px 0; border-bottom: 1px solid #0f3460; }
  .metric:last-child { border-bottom: none; }
  .label { color: #a0a0a0; font-size: 14px; }
  .value { font-size: 28px; font-weight: bold; color: #00d4aa; }
  .unit { font-size: 14px; color: #888; margin-left: 4px; }
  button { background: #e94560; color: white; border: none; padding: 14px 40px; font-size: 16px; border-radius: 8px; cursor: pointer; display: block; margin: 20px auto; }
  button:hover { background: #ff6b81; }
  button:disabled { background: #555; cursor: not-allowed; }
  #log { background: #0f3460; border-radius: 8px; padding: 12px; font-family: monospace; font-size: 12px; max-height: 200px; overflow-y: auto; white-space: pre-wrap; }
  .status { text-align: center; color: #ffd700; margin: 10px 0; min-height: 20px; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  .big-value { font-size: 42px; }
</style>
</head>
<body>
<h1>Speed Test</h1>
<div class="status" id="status">Click Start to begin continuous testing</div>
<button id="btn" onclick="toggleTest()">Start Test</button>

<div class="grid">
  <div class="card">
    <div class="metric">
      <span class="label">Download</span>
      <span><span class="value big-value" id="dl">0.0</span><span class="unit">Mbps</span></span>
    </div>
  </div>
  <div class="card">
    <div class="metric">
      <span class="label">Upload</span>
      <span><span class="value big-value" id="ul">0.0</span><span class="unit">Mbps</span></span>
    </div>
  </div>
</div>

<div class="card">
  <div class="metric">
    <span class="label">Ping</span>
    <span><span class="value" id="ping">0</span><span class="unit">ms</span></span>
  </div>
  <div class="metric">
    <span class="label">Jitter</span>
    <span><span class="value" id="jitter">0</span><span class="unit">ms</span></span>
  </div>
  <div class="metric">
    <span class="label">Tests Run</span>
    <span><span class="value" id="count">0</span></span>
  </div>
</div>

<div class="card">
  <div class="label">Log</div>
  <div id="log"></div>
</div>

<script>
let running = false;
let testCount = 0;
let pings = [];

function log(msg) {
  const el = document.getElementById('log');
  el.textContent += msg + '\\n';
  el.scrollTop = el.scrollHeight;
}

async function measurePing() {
  const t0 = performance.now();
  const resp = await fetch('/ping?' + Math.random(), { cache: 'no-store' });
  if (!resp.ok) throw new Error('Ping HTTP ' + resp.status);
  await resp.text();
  const t1 = performance.now();
  return t1 - t0;
}

async function measureDownload() {
  const sizeMB = 8;
  const t0 = performance.now();
  const resp = await fetch('/download?r=' + Math.random(), { cache: 'no-store' });
  if (!resp.ok) throw new Error('Download HTTP ' + resp.status);
  await resp.arrayBuffer();
  const t1 = performance.now();
  const sec = (t1 - t0) / 1000;
  return (sizeMB * 8) / sec;
}

async function measureUpload() {
  const sizeMB = 4;
  const data = new Uint8Array(sizeMB * 1024 * 1024);
  for (let i = 0; i < data.length; i++) data[i] = Math.floor(Math.random() * 256);
  const t0 = performance.now();
  const resp = await fetch('/upload?r=' + Math.random(), {
    method: 'POST',
    body: data,
    headers: { 'Content-Type': 'application/octet-stream' },
    cache: 'no-store'
  });
  if (!resp.ok) throw new Error('Upload HTTP ' + resp.status);
  await resp.text();
  const t1 = performance.now();
  const sec = (t1 - t0) / 1000;
  return (sizeMB * 8) / sec;
}

async function runOnce() {
  document.getElementById('status').textContent = 'Testing...';
  const ping = await measurePing();
  pings.push(ping);
  if (pings.length > 20) pings.shift();
  const avg = pings.reduce((a,b)=>a+b,0) / pings.length;
  const jitter = pings.length > 1
    ? Math.sqrt(pings.reduce((s, p) => s + Math.pow(p - avg, 2), 0) / pings.length)
    : 0;
  const dl = await measureDownload();
  const ul = await measureUpload();
  testCount++;
  document.getElementById('dl').textContent = dl.toFixed(1);
  document.getElementById('ul').textContent = ul.toFixed(1);
  document.getElementById('ping').textContent = ping.toFixed(1);
  document.getElementById('jitter').textContent = jitter.toFixed(1);
  document.getElementById('count').textContent = testCount;
  log('#' + testCount + ' DL=' + dl.toFixed(1) + ' UL=' + ul.toFixed(1) + ' Ping=' + ping.toFixed(1) + 'ms Jitter=' + jitter.toFixed(1) + 'ms');
  document.getElementById('status').textContent = 'Waiting 2s before next test...';
}

async function loop() {
  while (running) {
    try {
      await runOnce();
    } catch (e) {
      log('Error: ' + e.message);
      document.getElementById('status').textContent = 'Error: ' + e.message;
    }
    if (running) await new Promise(r => setTimeout(r, 2000));
  }
  document.getElementById('status').textContent = 'Stopped';
}

function toggleTest() {
  running = !running;
  document.getElementById('btn').textContent = running ? 'Stop Test' : 'Start Test';
  if (running) {
    document.getElementById('status').textContent = 'Starting...';
    loop();
  }
}
</script>
</body>
</html>
"""


class SpeedTestHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        pass

    def _send_cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', '*')

    def do_OPTIONS(self):
        self.send_response(204)
        self._send_cors()
        self.send_header('Content-Length', '0')
        self.end_headers()

    def do_GET(self):
        path = self.path.split('?')[0]
        try:
            if path == '/':
                body = HTML_PAGE.encode('utf-8')
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self._send_cors()
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif path == '/ping':
                body = b'pong'
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain')
                self._send_cors()
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif path == '/download':
                total = DOWNLOAD_MB * 1024 * 1024
                self.send_response(200)
                self.send_header('Content-Type', 'application/octet-stream')
                self.send_header('Content-Length', str(total))
                self.send_header('Cache-Control', 'no-store')
                self._send_cors()
                self.end_headers()
                chunk = os.urandom(256 * 1024)
                repeats = total // len(chunk)
                for _ in range(repeats):
                    self.wfile.write(chunk)
            else:
                self.send_response(404)
                self.end_headers()
        except (ConnectionResetError, BrokenPipeError):
            pass

    def do_POST(self):
        path = self.path.split('?')[0]
        try:
            if path == '/upload':
                length = int(self.headers.get('Content-Length', 0))
                while length > 0:
                    to_read = min(length, 65536)
                    data = self.rfile.read(to_read)
                    if not data:
                        break
                    length -= len(data)
                body = b'ok'
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain')
                self._send_cors()
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()
        except (ConnectionResetError, BrokenPipeError):
            pass


class ReusableTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == '__main__':
    with ReusableTCPServer(("0.0.0.0", PORT), SpeedTestHandler) as httpd:
        print(f"Speed test server running at http://0.0.0.0:{PORT}/")
        httpd.serve_forever()
