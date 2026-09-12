"""
智能代理共享 - 手机端服务端 APP
基于 Kivy 的安卓应用，提供 HTTP/HTTPS 代理服务和 PAC 智能分流
"""

import os
import sys
import socket
import threading
import time
import json

# 代理服务器核心代码（内联版，避免外部依赖）
BUFFER_SIZE = 8192
CONNECTION_TIMEOUT = 300

# 国内直连域名
CHINA_DOMAINS = [
    ".cn", ".com.cn", ".net.cn", ".org.cn", ".gov.cn", ".edu.cn", ".ac.cn",
    "baidu.com", "taobao.com", "tmall.com", "jd.com", "qq.com", "weibo.com",
    "bilibili.com", "zhihu.com", "douban.com", "163.com", "sina.com.cn",
    "sohu.com", "ifeng.com", "xinhuanet.com", "people.com.cn",
    "aliyun.com", "huaweicloud.com", "tencent.com", "meituan.com",
    "dianping.com", "pinduoduo.com", "suning.com", "vip.com",
    "xiaohongshu.com", "kuaishou.com", "douyin.com", "toutiao.com",
    "feishu.cn", "dingtalk.com", "work.weixin.qq.com",
    "youku.com", "iqiyi.com", "mgtv.com", "le.com",
    "music.163.com", "y.qq.com", "kugou.com", "ximalaya.com",
    "ctrip.com", "qunar.com", "12306.cn", "amap.com",
    "csdn.net", "juejin.cn", "segmentfault.com", "cnblogs.com",
    "oschina.net", "jianshu.com", "ithome.com", "36kr.com",
    "huxiu.com", "thepaper.cn", "huanqiu.com",
    "alipay.com", "icbc.com.cn", "ccb.com", "boc.cn",
    "10.", "172.16.", "172.17.", "172.18.", "172.19.", "172.20.",
    "172.21.", "172.22.", "172.23.", "172.24.", "172.25.", "172.26.",
    "172.27.", "172.28.", "172.29.", "172.30.", "172.31.",
    "192.168.", "127.", "localhost",
]


class ProxyStats:
    def __init__(self):
        self.total_bytes_sent = 0
        self.total_bytes_received = 0
        self.active_connections = 0
        self.total_connections = 0
        self.start_time = None
        self.clients = {}
        self._lock = threading.Lock()

    def reset(self):
        with self._lock:
            self.total_bytes_sent = 0
            self.total_bytes_received = 0
            self.active_connections = 0
            self.total_connections = 0
            self.start_time = time.time()
            self.clients.clear()

    def record_client(self, client_ip, bytes_sent, bytes_received):
        with self._lock:
            if client_ip not in self.clients:
                self.clients[client_ip] = {
                    "connections": 0, "bytes_sent": 0,
                    "bytes_received": 0, "last_seen": time.time()
                }
            self.clients[client_ip]["connections"] += 1
            self.clients[client_ip]["bytes_sent"] += bytes_sent
            self.clients[client_ip]["bytes_received"] += bytes_received
            self.clients[client_ip]["last_seen"] = time.time()

    def add_connection(self):
        with self._lock:
            self.active_connections += 1
            self.total_connections += 1

    def remove_connection(self):
        with self._lock:
            self.active_connections -= 1

    def add_bytes(self, sent, received):
        with self._lock:
            self.total_bytes_sent += sent
            self.total_bytes_received += received

    def to_dict(self):
        with self._lock:
            uptime = int(time.time() - self.start_time) if self.start_time else 0
            return {
                "total_bytes_sent": self.total_bytes_sent,
                "total_bytes_received": self.total_bytes_received,
                "active_connections": self.active_connections,
                "total_connections": self.total_connections,
                "uptime": uptime,
                "client_count": len(self.clients),
            }


class ProxyServer:
    def __init__(self, host="0.0.0.0", port=8080, stats_callback=None):
        self.host = host
        self.port = port
        self.stats_callback = stats_callback
        self.server_socket = None
        self.running = False
        self.thread = None
        self.stats = ProxyStats()

    def start(self):
        if self.running:
            return False
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(128)
            self.server_socket.settimeout(1.0)
            self.running = True
            self.stats.reset()
            self.thread = threading.Thread(target=self._accept_loop, daemon=True)
            self.thread.start()
            return True
        except OSError:
            self.server_socket = None
            return False

    def stop(self):
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass
            self.server_socket = None

    def _accept_loop(self):
        while self.running:
            try:
                client_socket, client_addr = self.server_socket.accept()
                client_socket.settimeout(CONNECTION_TIMEOUT)
                self.stats.add_connection()
                t = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, client_addr),
                    daemon=True
                )
                t.start()
            except socket.timeout:
                continue
            except OSError:
                break
            if self.stats_callback:
                try:
                    self.stats_callback(self.stats.to_dict())
                except Exception:
                    pass

    def _handle_client(self, client_socket, client_addr):
        client_ip = client_addr[0]
        try:
            request_line_raw = self._read_line(client_socket)
            if not request_line_raw:
                return
            request_line = request_line_raw.decode("utf-8", errors="replace")
            parts = request_line.split()
            if len(parts) < 2:
                return
            method = parts[0].upper()
            target = parts[1]

            if method == "CONNECT":
                self._handle_connect(client_socket, client_ip, target)
            else:
                self._handle_http(client_socket, client_ip, method, target, request_line_raw)
        except Exception:
            pass
        finally:
            try:
                client_socket.close()
            except Exception:
                pass
            self.stats.remove_connection()
            if self.stats_callback:
                try:
                    self.stats_callback(self.stats.to_dict())
                except Exception:
                    pass

    def _handle_connect(self, client_socket, client_ip, target):
        try:
            host, port = target.split(":")
            port = int(port)
        except ValueError:
            self._send(client_socket, b"HTTP/1.1 400 Bad Request\r\n\r\n")
            return

        try:
            while True:
                line = self._read_line(client_socket)
                if not line or line == b"\r\n":
                    break
        except Exception:
            pass

        try:
            remote_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            remote_socket.settimeout(CONNECTION_TIMEOUT)
            remote_socket.connect((host, port))
        except Exception:
            self._send(client_socket, b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            return

        self._send(client_socket, b"HTTP/1.1 200 Connection Established\r\n\r\n")
        self._tunnel_data(client_socket, remote_socket, client_ip)

        try:
            remote_socket.close()
        except Exception:
            pass

    def _handle_http(self, client_socket, client_ip, method, target, request_line_raw):
        try:
            if target.startswith("http://"):
                target = target[7:]
            if "/" in target:
                host_part, path = target.split("/", 1)
                path = "/" + path
            else:
                host_part = target
                path = "/"
            if ":" in host_part:
                host, port = host_part.split(":", 1)
                port = int(port)
            else:
                host = host_part
                port = 80

            headers = []
            while True:
                line = self._read_line(client_socket)
                if not line or line == b"\r\n":
                    break
                headers.append(line)

            remote_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            remote_socket.settimeout(CONNECTION_TIMEOUT)
            remote_socket.connect((host, port))

            new_request = f"{method} {path} HTTP/1.1\r\n".encode()
            remote_socket.send(new_request)
            for header in headers:
                if not header.lower().startswith(b"proxy-connection"):
                    remote_socket.send(header)
            remote_socket.send(b"\r\n")

            self._tunnel_data(client_socket, remote_socket, client_ip)
            try:
                remote_socket.close()
            except Exception:
                pass
        except Exception:
            try:
                self._send(client_socket, b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            except Exception:
                pass

    def _tunnel_data(self, client_socket, remote_socket, client_ip):
        bytes_sent = 0
        bytes_received = 0
        try:
            import select
            while True:
                rlist, _, xlist = select.select(
                    [client_socket, remote_socket], [],
                    [client_socket, remote_socket], 60
                )
                if xlist or not rlist:
                    break
                for sock in rlist:
                    try:
                        data = sock.recv(BUFFER_SIZE)
                    except Exception:
                        return
                    if not data:
                        return
                    if sock is client_socket:
                        remote_socket.sendall(data)
                        bytes_sent += len(data)
                    else:
                        client_socket.sendall(data)
                        bytes_received += len(data)
        except Exception:
            pass
        finally:
            self.stats.add_bytes(bytes_sent, bytes_received)
            self.stats.record_client(client_ip, bytes_sent, bytes_received)

    def _read_line(self, sock):
        line = b""
        while True:
            try:
                char = sock.recv(1)
            except socket.timeout:
                return line
            except Exception:
                return line
            if not char:
                return line
            line += char
            if line.endswith(b"\r\n"):
                return line
            if len(line) > 8192:
                return line

    def _send(self, sock, data):
        try:
            sock.sendall(data)
        except Exception:
            pass


# PAC 文件生成
def generate_pac(proxy_host, proxy_port):
    direct_list = ", ".join(f'"{d}"' for d in CHINA_DOMAINS)
    return f"""function FindProxyForURL(url, host) {{
    var proxy = "PROXY {proxy_host}:{proxy_port}";
    var direct = "DIRECT";
    var directDomains = [{direct_list}];
    for (var i = 0; i < directDomains.length; i++) {{
        var d = directDomains[i];
        if (d.indexOf(".") === 0) {{
            if (host.endsWith(d)) return direct;
        }} else if (d.indexOf(".") > -1) {{
            if (dnsDomainIs(host, d) || host == d) return direct;
        }} else {{
            if (host.indexOf(d) === 0) return direct;
        }}
    }}
    if (isPlainHostName(host)) return direct;
    if (isInNet(host, "10.0.0.0", "255.0.0.0")) return direct;
    if (isInNet(host, "172.16.0.0", "255.240.0.0")) return direct;
    if (isInNet(host, "192.168.0.0", "255.255.0.0")) return direct;
    if (isInNet(host, "127.0.0.0", "255.0.0.0")) return direct;
    return proxy;
}}"""


# 简单的 HTTP 服务器，提供 Web 管理面板和 PAC 文件
class SimpleHTTPServer:
    def __init__(self, host="0.0.0.0", port=5000, proxy_port=8080, get_stats=None):
        self.host = host
        self.port = port
        self.proxy_port = proxy_port
        self.get_stats = get_stats
        self.server_socket = None
        self.running = False
        self.thread = None

    def start(self):
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(10)
            self.server_socket.settimeout(1.0)
            self.running = True
            self.thread = threading.Thread(target=self._loop, daemon=True)
            self.thread.start()
            return True
        except Exception:
            return False

    def stop(self):
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass
            self.server_socket = None

    def _loop(self):
        while self.running:
            try:
                client, addr = self.server_socket.accept()
                t = threading.Thread(target=self._handle, args=(client, addr), daemon=True)
                t.start()
            except socket.timeout:
                continue
            except Exception:
                break

    def _handle(self, client, addr):
        try:
            client.settimeout(10)
            request = b""
            while b"\r\n\r\n" not in request:
                chunk = client.recv(1024)
                if not chunk:
                    break
                request += chunk
                if len(request) > 8192:
                    break

            request_text = request.decode("utf-8", errors="replace")
            lines = request_text.split("\r\n")
            if not lines:
                client.close()
                return

            first_line = lines[0]
            parts = first_line.split()
            if len(parts) < 2:
                client.close()
                return

            path = parts[1]

            if path == "/pac" or path.startswith("/pac?"):
                # PAC 文件
                pac_content = generate_pac(addr[0], self.proxy_port)
                response = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: application/x-ns-proxy-autoconfig\r\n"
                    b"Cache-Control: no-cache\r\n"
                    f"Content-Length: {len(pac_content)}\r\n\r\n"
                ) + pac_content.encode()
                client.sendall(response)
            elif path == "/" or path == "/index.html":
                # 简单的状态页
                stats = self.get_stats() if self.get_stats else {}
                html = self._generate_status_page(stats)
                response = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: text/html; charset=utf-8\r\n"
                    f"Content-Length: {len(html)}\r\n\r\n"
                ) + html.encode("utf-8")
                client.sendall(response)
            elif path == "/api/status":
                # API 状态
                stats = self.get_stats() if self.get_stats else {}
                import json
                body = json.dumps(stats)
                response = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: application/json\r\n"
                    f"Content-Length: {len(body)}\r\n\r\n"
                ) + body.encode()
                client.sendall(response)
            else:
                response = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n"
                client.sendall(response)
        except Exception:
            pass
        finally:
            try:
                client.close()
            except Exception:
                pass

    def _generate_status_page(self, stats):
        sent = stats.get("total_bytes_sent", 0)
        received = stats.get("total_bytes_received", 0)
        active = stats.get("active_connections", 0)
        total = stats.get("total_connections", 0)
        uptime = stats.get("uptime", 0)
        clients = stats.get("client_count", 0)

        def fmt_bytes(b):
            if b < 1024:
                return f"{b} B"
            if b < 1024 * 1024:
                return f"{b / 1024:.1f} KB"
            if b < 1024 * 1024 * 1024:
                return f"{b / 1024 / 1024:.1f} MB"
            return f"{b / 1024 / 1024 / 1024:.2f} GB"

        def fmt_time(s):
            h = s // 3600
            m = (s % 3600) // 60
            sec = s % 60
            if h > 0:
                return f"{h}h {m}m"
            if m > 0:
                return f"{m}m {sec}s"
            return f"{sec}s"

        return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>智能代理共享</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:sans-serif;background:linear-gradient(135deg,#667eea,#764ba2);min-height:100vh;padding:16px;color:#333}}
.card{{background:white;border-radius:16px;padding:20px;margin-bottom:16px;box-shadow:0 4px 20px rgba(0,0,0,.15)}}
h1{{color:white;text-align:center;font-size:1.5em;margin-bottom:16px}}
h2{{font-size:1.1em;margin-bottom:12px;color:#333}}
.status{{text-align:center;padding:20px}}
.status-dot{{display:inline-block;width:14px;height:14px;border-radius:50%;background:#10b981;box-shadow:0 0 10px #10b981;margin-right:8px;animation:pulse 2s infinite}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.5}}}}
.status-text{{font-size:1.2em;font-weight:bold;color:#10b981}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:12px}}
.stat{{text-align:center;padding:12px;background:#f8f9fa;border-radius:12px}}
.stat-value{{font-size:1.4em;font-weight:bold;color:#667eea}}
.stat-label{{font-size:.8em;color:#888;margin-top:4px}}
.pac{{background:#f0fdf4;border:1px solid #bbf7d0;border-radius:12px;padding:14px;margin-top:12px}}
.pac-url{{background:white;padding:10px;border-radius:8px;font-family:monospace;font-size:.85em;word-break:break-all;margin:8px 0}}
.info{{font-size:.85em;color:#666;line-height:1.6}}
</style></head><body>
<h1>🌐 智能代理共享</h1>
<div class="card">
<div class="status"><span class="status-dot"></span><span class="status-text">代理运行中</span></div>
<div class="grid">
<div class="stat"><div class="stat-value">{fmt_bytes(sent)}</div><div class="stat-label">上行流量</div></div>
<div class="stat"><div class="stat-value">{fmt_bytes(received)}</div><div class="stat-label">下行流量</div></div>
<div class="stat"><div class="stat-value">{active}</div><div class="stat-label">活跃连接</div></div>
<div class="stat"><div class="stat-value">{total}</div><div class="stat-label">总连接数</div></div>
<div class="stat"><div class="stat-value">{clients}</div><div class="stat-label">连接设备</div></div>
<div class="stat"><div class="stat-value">{fmt_time(uptime)}</div><div class="stat-label">运行时间</div></div>
</div>
</div>
<div class="card">
<h2>📋 PAC 智能分流</h2>
<p class="info">将下方地址填入电脑代理设置的"自动代理配置 URL"</p>
<div class="pac-url" id="pacUrl">加载中...</div>
<p class="info" style="color:#059669">国内网站直连，国外网站自动走代理</p>
</div>
<div class="card">
<h2>💡 使用说明</h2>
<p class="info">
1. 确保手机和电脑在同一 WiFi<br>
2. 电脑设置 → 网络 → 代理 → 使用设置脚本<br>
3. 填入上方 PAC 地址，保存<br>
4. 国内网站直连，国外自动走代理
</p>
</div>
<script>
var url = window.location.origin + '/pac';
document.getElementById('pacUrl').textContent = url;
setTimeout(function(){{location.reload()}}, 5000);
</script>
</body></html>"""


# 获取本机 IP
def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "127.0.0.1"


# ==================== Kivy APP ====================

try:
    from kivy.app import App
    from kivy.uix.boxlayout import BoxLayout
    from kivy.uix.button import Button
    from kivy.uix.label import Label
    from kivy.uix.textinput import TextInput
    from kivy.uix.screenmanager import ScreenManager, Screen
    from kivy.uix.scrollview import ScrollView
    from kivy.uix.gridlayout import GridLayout
    from kivy.clock import Clock
    from kivy.core.window import Window
    from kivy.metrics import dp
    from kivy.graphics import Color, RoundedRectangle
    from kivy.uix.widget import Widget

    KIVY_AVAILABLE = True
except ImportError:
    KIVY_AVAILABLE = False


if KIVY_AVAILABLE:
    # 设置窗口（调试用）
    Window.clearcolor = (0.96, 0.97, 0.98, 1)

    class CardWidget(BoxLayout):
        """圆角卡片组件"""
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.orientation = "vertical"
            self.padding = dp(16)
            self.spacing = dp(8)
            with self.canvas.before:
                Color(1, 1, 1, 1)
                self.rect = RoundedRectangle(radius=[dp(12), dp(12), dp(12), dp(12)])
            self.bind(pos=self._update_rect, size=self._update_rect)

        def _update_rect(self, *args):
            self.rect.pos = self.pos
            self.rect.size = self.size

    class MainScreen(Screen):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.name = "main"
            self.proxy_server = None
            self.web_server = None
            self.is_running = False
            self.stats = {}

            layout = BoxLayout(orientation="vertical", padding=dp(20), spacing=dp(16))

            # 标题
            title_label = Label(
                text="🌐 智能代理共享",
                font_size=dp(22),
                bold=True,
                color=(0.2, 0.2, 0.2, 1),
                size_hint_y=None,
                height=dp(40)
            )
            layout.add_widget(title_label)

            subtitle = Label(
                text="手机 VPN 共享给电脑",
                font_size=dp(13),
                color=(0.4, 0.4, 0.4, 1),
                size_hint_y=None,
                height=dp(24)
            )
            layout.add_widget(subtitle)

            # 状态卡片
            status_card = CardWidget(size_hint_y=None, height=dp(120))
            self.status_label = Label(
                text="● 代理未启动",
                font_size=dp(18),
                bold=True,
                color=(0.6, 0.6, 0.6, 1),
                size_hint_y=None,
                height=dp(32)
            )
            status_card.add_widget(self.status_label)

            self.ip_label = Label(
                text="手机 IP: 检测中...",
                font_size=dp(14),
                color=(0.3, 0.3, 0.3, 1),
                size_hint_y=None,
                height=dp(28)
            )
            status_card.add_widget(self.ip_label)

            self.port_label = Label(
                text="代理端口: 8080",
                font_size=dp(13),
                color=(0.5, 0.5, 0.5, 1),
                size_hint_y=None,
                height=dp(24)
            )
            status_card.add_widget(self.port_label)

            layout.add_widget(status_card)

            # 端口设置
            port_card = CardWidget(size_hint_y=None, height=dp(80))
            port_card.add_widget(Label(
                text="代理端口设置",
                font_size=dp(14),
                bold=True,
                color=(0.2, 0.2, 0.2, 1),
                size_hint_y=None,
                height=dp(24),
                halign="left",
                text_size=(dp(300), None)
            ))
            port_input_layout = BoxLayout(
                orientation="horizontal",
                size_hint_y=None,
                height=dp(40),
                spacing=dp(8)
            )
            self.port_input = TextInput(
                text="8080",
                font_size=dp(16),
                multiline=False,
                input_filter="int",
                size_hint_x=0.6,
                padding=[dp(10), dp(8)]
            )
            port_input_layout.add_widget(self.port_input)

            self.web_port_input = TextInput(
                text="5000",
                font_size=dp(16),
                multiline=False,
                input_filter="int",
                size_hint_x=0.4,
                padding=[dp(10), dp(8)]
            )
            port_input_layout.add_widget(self.web_port_input)
            port_card.add_widget(port_input_layout)

            # 端口标签
            port_labels = BoxLayout(
                orientation="horizontal",
                size_hint_y=None,
                height=dp(20),
                spacing=dp(8)
            )
            port_labels.add_widget(Label(
                text="代理端口", font_size=dp(11),
                color=(0.5, 0.5, 0.5, 1), size_hint_x=0.6
            ))
            port_labels.add_widget(Label(
                text="管理端口", font_size=dp(11),
                color=(0.5, 0.5, 0.5, 1), size_hint_x=0.4
            ))
            port_card.add_widget(port_labels)

            layout.add_widget(port_card)

            # 统计卡片
            stats_card = CardWidget(size_hint_y=None, height=dp(160))
            stats_card.add_widget(Label(
                text="📊 流量统计",
                font_size=dp(14),
                bold=True,
                color=(0.2, 0.2, 0.2, 1),
                size_hint_y=None,
                height=dp(24),
                halign="left",
                text_size=(dp(300), None)
            ))

            stats_grid = GridLayout(
                cols=3,
                spacing=dp(8),
                size_hint_y=None,
                height=dp(100)
            )
            self.sent_label = self._create_stat_label("0 B", "上行")
            self.received_label = self._create_stat_label("0 B", "下行")
            self.active_label = self._create_stat_label("0", "活跃")
            self.total_label = self._create_stat_label("0", "总连接")
            self.clients_label = self._create_stat_label("0", "设备数")
            self.uptime_label = self._create_stat_label("0s", "运行")
            stats_grid.add_widget(self.sent_label)
            stats_grid.add_widget(self.received_label)
            stats_grid.add_widget(self.active_label)
            stats_grid.add_widget(self.total_label)
            stats_grid.add_widget(self.clients_label)
            stats_grid.add_widget(self.uptime_label)
            stats_card.add_widget(stats_grid)

            layout.add_widget(stats_card)

            # PAC 提示
            pac_card = CardWidget(size_hint_y=None, height=dp(90))
            pac_card.add_widget(Label(
                text="🎯 PAC 智能分流",
                font_size=dp(14),
                bold=True,
                color=(0.05, 0.6, 0.3, 1),
                size_hint_y=None,
                height=dp(24),
                halign="left",
                text_size=(dp(300), None)
            ))
            self.pac_label = Label(
                text="启动后显示 PAC 地址",
                font_size=dp(11),
                color=(0.4, 0.4, 0.4, 1),
                size_hint_y=None,
                height=dp(40),
                text_size=(dp(300), None),
                halign="left"
            )
            pac_card.add_widget(self.pac_label)
            layout.add_widget(pac_card)

            # 启动按钮
            self.toggle_btn = Button(
                text="🚀 启动代理服务",
                font_size=dp(18),
                bold=True,
                size_hint_y=None,
                height=dp(56),
                background_color=(0.4, 0.49, 0.92, 1),
                background_normal="",
                background_down="",
                color=(1, 1, 1, 1),
                on_press=self.toggle_proxy
            )
            # 圆角按钮
            with self.toggle_btn.canvas.before:
                Color(0.4, 0.49, 0.92, 1)
                self.btn_rect = RoundedRectangle(radius=[dp(28)])
            self.toggle_btn.bind(pos=self._update_btn, size=self._update_btn)

            layout.add_widget(self.toggle_btn)

            # 底部提示
            tip_label = Label(
                text="💡 确保手机和电脑连接同一 WiFi",
                font_size=dp(12),
                color=(0.6, 0.5, 0.1, 1),
                size_hint_y=None,
                height=dp(24)
            )
            layout.add_widget(tip_label)

            self.add_widget(layout)

            # 初始化
            Clock.schedule_once(self.init_ip, 0.5)
            Clock.schedule_interval(self.update_stats, 2)

        def _create_stat_label(self, value, label_text):
            box = BoxLayout(orientation="vertical", padding=[dp(4)])
            with box.canvas.before:
                Color(0.97, 0.97, 0.98, 1)
                rect = RoundedRectangle(radius=[dp(8)])
            box.bind(pos=lambda *a: setattr(rect, 'pos', box.pos),
                     size=lambda *a: setattr(rect, 'size', box.size))

            val_label = Label(
                text=value,
                font_size=dp(15),
                bold=True,
                color=(0.4, 0.49, 0.92, 1),
                size_hint_y=0.6
            )
            box.add_widget(val_label)

            lbl = Label(
                text=label_text,
                font_size=dp(10),
                color=(0.5, 0.5, 0.5, 1),
                size_hint_y=0.4
            )
            box.add_widget(lbl)

            # 保存引用
            box.value_label = val_label
            return box

        def _update_btn(self, *args):
            self.btn_rect.pos = self.toggle_btn.pos
            self.btn_rect.size = self.toggle_btn.size

        def init_ip(self, dt):
            ip = get_local_ip()
            self.ip_label.text = f"手机 IP: {ip}"

        def toggle_proxy(self, instance):
            if not self.is_running:
                self.start_proxy()
            else:
                self.stop_proxy()

        def start_proxy(self):
            try:
                port = int(self.port_input.text)
                web_port = int(self.web_port_input.text)
            except ValueError:
                return

            self.proxy_server = ProxyServer(
                host="0.0.0.0",
                port=port,
                stats_callback=self.on_stats_update
            )
            success = self.proxy_server.start()

            if not success:
                self.status_label.text = "● 启动失败"
                self.status_label.color = (0.9, 0.2, 0.2, 1)
                return

            # 启动 Web 管理服务
            self.web_server = SimpleHTTPServer(
                host="0.0.0.0",
                port=web_port,
                proxy_port=port,
                get_stats=lambda: self.proxy_server.stats.to_dict() if self.proxy_server else {}
            )
            self.web_server.start()

            self.is_running = True
            self.status_label.text = "● 代理运行中"
            self.status_label.color = (0.06, 0.73, 0.51, 1)
            self.toggle_btn.text = "🛑 停止代理服务"

            ip = get_local_ip()
            self.pac_label.text = f"PAC 地址: http://{ip}:{web_port}/pac\n电脑设置自动代理配置即可"

        def stop_proxy(self):
            if self.proxy_server:
                self.proxy_server.stop()
                self.proxy_server = None
            if self.web_server:
                self.web_server.stop()
                self.web_server = None

            self.is_running = False
            self.status_label.text = "● 代理已停止"
            self.status_label.color = (0.6, 0.6, 0.6, 1)
            self.toggle_btn.text = "🚀 启动代理服务"
            self.pac_label.text = "启动后显示 PAC 地址"

        def on_stats_update(self, stats):
            self.stats = stats

        def update_stats(self, dt):
            if self.proxy_server:
                stats = self.proxy_server.stats.to_dict()
                self.stats = stats
                self._update_stat_labels(stats)

        def _update_stat_labels(self, stats):
            def fmt_bytes(b):
                if b < 1024:
                    return f"{b}B"
                if b < 1024 * 1024:
                    return f"{b / 1024:.1f}K"
                if b < 1024 * 1024 * 1024:
                    return f"{b / 1024 / 1024:.1f}M"
                return f"{b / 1024 / 1024 / 1024:.1f}G"

            def fmt_time(s):
                h = s // 3600
                m = (s % 3600) // 60
                sec = s % 60
                if h > 0:
                    return f"{h}h{m}m"
                if m > 0:
                    return f"{m}m{sec}s"
                return f"{sec}s"

            self.sent_label.value_label.text = fmt_bytes(stats.get("total_bytes_sent", 0))
            self.received_label.value_label.text = fmt_bytes(stats.get("total_bytes_received", 0))
            self.active_label.value_label.text = str(stats.get("active_connections", 0))
            self.total_label.value_label.text = str(stats.get("total_connections", 0))
            self.clients_label.value_label.text = str(stats.get("client_count", 0))
            self.uptime_label.value_label.text = fmt_time(stats.get("uptime", 0))

    class ProxyApp(App):
        def build(self):
            self.title = "智能代理共享"
            sm = ScreenManager()
            sm.add_widget(MainScreen(name="main"))
            return sm


def main():
    """主入口"""
    if KIVY_AVAILABLE:
        ProxyApp().run()
    else:
        # 没有 Kivy 时用命令行模式
        print("=" * 50)
        print("🌐 智能代理共享 - 手机服务端")
        print("=" * 50)
        print(f"📱 本机 IP: {get_local_ip()}")

        proxy = ProxyServer(host="0.0.0.0", port=8080)
        web = SimpleHTTPServer(host="0.0.0.0", port=5000, proxy_port=8080,
                               get_stats=lambda: proxy.stats.to_dict())

        if proxy.start():
            web.start()
            print("✅ 代理服务已启动 (端口 8080)")
            print("🌐 Web 管理: http://" + get_local_ip() + ":5000")
            print("📋 PAC 地址: http://" + get_local_ip() + ":5000/pac")
            print("\n按 Ctrl+C 停止...")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\n正在停止...")
                proxy.stop()
                web.stop()
                print("已停止")
        else:
            print("❌ 启动失败，端口可能被占用")


if __name__ == "__main__":
    main()
