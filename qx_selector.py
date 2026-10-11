#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
qx_selector.py — 公开节点池 · 港/台/日/美 延迟优选 · 输出 QuantumultX 订阅

流程：
  1) 拉取多个社区公开节点源（免账号）
  2) 解析协议 + 按地区归类（港/TW/日/美，支持节点名关键词 + IP 归属地兜底）
  3) TCP 握手 RTT 初筛（并发、多轮取最快）
  4) 用 Xray 核心做真实代理层延迟复测（HTTP 204 实测，能排除"端口通但连不上"的假活节点）
  5) 每个地区按实测延迟排序取最快，总条数上限 100
  6) 合成 QuantumultX 可直接用的订阅内容（QX 原生 server 格式）

产物（默认 out_qx/）：
  qx_sub.txt           QX 订阅内容（明文，QX 里"引用 → 节点"填这个文件的 URL 即可）
  qx_sub_base64.txt    base64 版（部分客户端/中转用）
  nodes_best.txt       选中节点的明文链接（一行一个）
  selection.json       结构化数据（含每个节点的实测延迟、地区、来源）
  report.txt           本次报告

用法：
  python3 qx_selector.py                       # 默认：港台日美，最多 100 条
  python3 qx_selector.py --per-region 30       # 每个地区取最快 30 条（总上限仍受 --total 约束）
  python3 qx_selector.py --total 50            # 总共只要 50 条
  python3 qx_selector.py --regions HK,JP,US    # 只要这三地
  python3 qx_selector.py --tcp-only            # 不做代理层复测，只按 TCP RTT 排（快很多）
  python3 qx_selector.py --interval 3600       # 常驻，每小时重新优选一次
  python3 qx_selector.py                       # 成功后自动更新固定 GitHub 订阅直链
  python3 qx_selector.py --publish-github off  # 只生成本地文件，不发布

固定 QX 引用链接：
  https://raw.githubusercontent.com/art61771-stack/qx-node-subscription/main/qx_sub.txt
发布认证：读取 GH_TOKEN/GITHUB_TOKEN，或复用本机 gh auth login；令牌不会写入脚本或订阅。

依赖：Python 3.8+；代理层复测需要 Xray 核心（没有会自动下载到 ./bin/，也可以 --xray 指定）
"""
import argparse, asyncio, base64, json, os, random, re, shutil, socket, stat, subprocess
import sys, tarfile, tempfile, time
import urllib.request
import concurrent.futures as cf

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/122.0 Safari/537.36")

# —— 公开源（社区每日自动更新，免账号）——
DEFAULT_SOURCES = {
    "mahdibland":    "https://raw.githubusercontent.com/mahdibland/V2RayAggregator/master/sub/sub_merge.txt",
    "aiboboxx":      "https://raw.githubusercontent.com/aiboboxx/v2rayfree/main/sub",
    "mfuu":          "https://raw.githubusercontent.com/mfuu/v2ray/master/sub",
    "nomorewalls":   "https://raw.githubusercontent.com/peasoft/NoMoreWalls/master/list.txt",
    "ripaojiedian":  "https://raw.githubusercontent.com/ripaojiedian/freenode/main/sub",
    "ermaozi":       "https://raw.githubusercontent.com/ermaozi/get_subscribe/main/subscribe/v2ray.txt",
    "barabama":      "https://raw.githubusercontent.com/Barabama/FreeNodes/main/nodes/nodefree.txt",
    "freefq":        "https://raw.githubusercontent.com/freefq/free/master/v2",
    "anaer_clash":   "https://raw.githubusercontent.com/anaer/Sub/main/clash.yaml",
    "ALIILAPRO":     "https://raw.githubusercontent.com/ALIILAPRO/v2rayNG-Config/main/server.txt",
    "roosterkid":    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/V2RAY_RAW.txt",
    "ts_sf":         "https://raw.githubusercontent.com/ts-sf/fly/main/v2",
    "eternity":      "https://raw.githubusercontent.com/mahdibland/ShadowsocksAggregator/master/Eternity.txt",
    "pawdroid":      "https://raw.githubusercontent.com/Pawdroid/Free-servers/main/sub",
    "epodonios":     "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/All_Configs_Sub.txt",
}

SCHEME_RE = re.compile(r'\b(?:ss|ssr|vmess|vless|trojan|hysteria2?|hy2|tuic)://[^\s"\'<>\\`,]+')
JUNK = re.compile(r'(剩余流量|套餐到期|到期时间|官网|订阅地址|购买|续费|重置|过期|流量：|'
                  r'Expire|Traffic|官网地址|机场官网|请勿|禁止|群组)', re.I)
PROTO_ALIAS = {"hy2": "hysteria2", "hysteria": "hysteria2"}

# 目标地区 + 关键词表（越大越优先匹配，先长后短）
REGION_KEYS = {
    "HK": ["香港", "hongkong", "hong kong", "🇭🇰", "hkg", "hk-", "-hk", "hk_", "_hk", " hk", "hk ", "深港", "沪港", "京港", "香港节点"],
    "TW": ["台湾", "台灣", "taiwan", "taipei", "台北", "新北", "彰化", "🇹🇼", "tpe", "tw-", "-tw", "tw_", "_tw", " tw", "tw "],
    "JP": ["日本", "japan", "tokyo", "东京", "東京", "大阪", "osaka", "埼玉", "🇯🇵", "nrt", "kix", "jp-", "-jp", "jp_", "_jp", " jp", "jp "],
    "US": ["美国", "美國", "united states", "los angeles", "san jose", "seattle", "硅谷", "洛杉矶", "洛杉磯",
           "圣何塞", "西雅图", "达拉斯", "凤凰城", "波特兰", "盐湖城", "拉斯维加斯", "芝加哥", "纽约",
           "🇺🇸", "lax", "sjc", "sea", "us-", "-us", "us_", "_us", " usa", "usa ", " us", "us "],
}

XRAY_VER = "v1.8.24"
XRAY_ASSETS = {
    "linux-64": "Xray-linux-64.zip", "linux-arm64": "Xray-linux-arm64-v8a.zip",
    "mac-64": "Xray-macOS-64.zip", "mac-arm64": "Xray-macOS-arm64-v8a.zip",
    "win-64": "Xray-windows-64.zip",
}
XRAY_URLS = [
    "https://github.com/XTLS/Xray-core/releases/download/%s/%%s" % XRAY_VER,
    "https://gh-proxy.com/https://github.com/XTLS/Xray-core/releases/download/%s/%%s" % XRAY_VER,
]


def xray_asset():
    import platform
    sysname = platform.system().lower()
    mach = platform.machine().lower()
    arm = mach in ("arm64", "aarch64") or "arm" in mach
    if sysname == "linux":
        return XRAY_ASSETS["linux-arm64" if arm else "linux-64"]
    if sysname == "darwin":
        return XRAY_ASSETS["mac-arm64" if arm else "mac-64"]
    if sysname == "windows":
        return XRAY_ASSETS["win-64"]
    return XRAY_ASSETS["linux-64"]

# —— 基础工具 ——
def log(msg):
    print("[%s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


def clean_name(s):
    if not s:
        return ""
    s = re.sub(r'[\x00-\x1f\x7f"\\`\'{}\[\],:=]', " ", str(s))
    s = re.sub(r'\s+', " ", s).strip()
    return s[:60]


def fetch(url, timeout=45):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        raw = urllib.request.urlopen(req, timeout=timeout).read()
    except Exception as e:
        return "", str(e)
    txt = raw.decode("utf-8", "ignore")
    if "://" not in txt:
        dec = try_b64(txt)
        if dec:
            txt = dec
    return txt, ""


def b64decode_any(value, text=True):
    """Decode standard or URL-safe base64, with strict UTF-8 by default."""
    raw = re.sub(r"\s+", "", str(value)).encode("ascii")
    raw += b"=" * (-len(raw) % 4)
    data = base64.b64decode(raw, altchars=b"-_", validate=False)
    return data.decode("utf-8") if text else data


def try_b64(text):
    s = re.sub(r'\s+', '', text)
    if len(s) < 32 or not re.fullmatch(r'[A-Za-z0-9+/=_\-]+', s):
        return ""
    try:
        d = b64decode_any(s)
    except Exception:
        return ""
    return d if "://" in d else ""


def extract(text):
    out = []
    for m in SCHEME_RE.finditer(text):
        out.append(m.group(0))
    if not out:
        d = try_b64(text)
        if d:
            for m in SCHEME_RE.finditer(d):
                out.append(m.group(0))
    return out


def extract_from_yaml(text):
    """从 clash yaml 里抽 proxies，转回链接（只处理我们能还原的）"""
    out = []
    try:
        import yaml
        doc = yaml.safe_load(text)
    except Exception:
        return out
    if not isinstance(doc, dict):
        return out
    for p in doc.get("proxies") or []:
        try:
            t = p.get("type")
            if t == "ss":
                u = base64.b64encode(("%s:%s" % (p["cipher"], p["password"])).encode()).decode()
                out.append("ss://%s@%s:%s#%s" % (u, p["server"], p["port"],
                                                urllib.request.quote(str(p.get("name") or ""))))
            elif t == "vmess":
                j = {"v": "2", "ps": p.get("name") or "", "add": p["server"], "port": str(p["port"]),
                     "id": p.get("uuid"), "aid": str(p.get("alterId") or 0), "scy": p.get("cipher") or "auto",
                     "net": p.get("network") or "tcp", "type": "none", "host": "", "path": "", "tls": ""}
                if p.get("tls"):
                    j["tls"] = "tls"
                    j["sni"] = p.get("servername") or ""
                ws = p.get("ws-opts") or {}
                if ws:
                    j["path"] = ws.get("path") or "/"
                    j["host"] = (ws.get("headers") or {}).get("Host") or ""
                b = base64.b64encode(json.dumps(j, ensure_ascii=False).encode()).decode()
                out.append("vmess://" + b)
        except Exception:
            continue
    return out


def parse_node(link):
    low = link.lower()
    proto = PROTO_ALIAS.get(low.split("://", 1)[0], low.split("://", 1)[0])
    name, host, port = "", "", 0
    if proto == "vmess":
        try:
            b = link[8:]
            j = json.loads(b64decode_any(b))
            host = str(j.get("add") or "").strip()
            port = int(j.get("port") or 0)
            name = str(j.get("ps") or "")
        except Exception:
            return None
    else:
        if "#" in link:
            name = urllib.request.unquote(link.split("#", 1)[1])
        m = re.search(r'@\[?([^\]@/?#]+?)\]?:(\d+)', link)
        if not m:
            m = re.search(r'://\[?([^\]@/?#]+?)\]?:(\d+)', link)
        if not m:
            m2 = re.search(r'://([A-Za-z0-9+/=_\-]{16,})', link)
            if m2:
                try:
                    dec = b64decode_any(m2.group(1))
                    m = re.search(r'@\[?([^\]@/?#]+?)\]?:(\d+)', dec)
                except Exception:
                    m = None
        if m:
            host, port = m.group(1), int(m.group(2))
    if not host or not port or not (0 < port < 65536) or not valid_host(host):
        return None
    return {"proto": proto, "name": name, "host": host, "port": port, "raw": link}


# 测速核心认的 ss 加密（旧式 cfb/chacha20/rc4 已被移除，单独测不了）
SS_CIPHERS_OK = {
    "aes-128-gcm", "aes-256-gcm", "chacha20-poly1305", "chacha20-ietf-poly1305",
    "xchacha20-poly1305", "none", "plain", "2022-blake3-aes-128-gcm",
    "2022-blake3-aes-256-gcm", "2022-blake3-chacha20-poly1305",
}


def valid_host(h):
    if not h or len(h) > 253:
        return False
    if re.fullmatch(r'\d{1,3}(\.\d{1,3}){3}', h):
        return all(int(x) < 256 for x in h.split("."))
    return bool(re.fullmatch(r'(?i)[a-z0-9]([a-z0-9\-_]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9\-_]{0,61}[a-z0-9])?)+', h))


# —— 协议解析：统一输出一个中间结构 ——
def to_mid(proto, n):
    """Parse only combinations that can be represented faithfully in QX."""
    raw, host, port = n["raw"], n["host"], n["port"]
    name = clean_name(n["name"]) or (host + ":" + str(port))
    from urllib.parse import urlparse, parse_qs, unquote

    if proto == "vmess":
        try:
            j = json.loads(b64decode_any(raw[8:]))
        except Exception:
            return None
        net = str(j.get("net") or "tcp").lower()
        if net not in ("ws", "tcp"):
            return None
        sec = str(j.get("tls") or "").lower()
        if sec == "reality":                 # vmess Reality fields vary too much in public feeds
            return None
        return {"type": "vmess", "server": host, "port": port, "uuid": str(j.get("id") or ""),
                "aid": int(j.get("aid") or 0), "cipher": str(j.get("scy") or "auto").lower(),
                "security": "tls" if sec in ("tls", "true", "1") else "none",
                "tls": sec in ("tls", "true", "1"), "reality": False, "network": net,
                "path": str(j.get("path") or "/"), "host_header": str(j.get("host") or ""),
                "sni": str(j.get("sni") or j.get("host") or ""), "name": name}

    try:
        u = urlparse(raw)
        qs0 = parse_qs(u.query, keep_blank_values=True)
        qs = {k.lower(): v[0] for k, v in qs0.items()}
    except Exception:
        return None
    q = lambda *keys, default="": next((qs[k.lower()] for k in keys if k.lower() in qs), default)
    net = q("type", default="tcp").lower()

    if proto == "vless":
        if net not in ("ws", "tcp"):
            return None
        security = q("security", default="none").lower()
        if security not in ("none", "tls", "reality"):
            return None
        pbk, sid = q("pbk", "publicKey"), q("sid", "shortId")
        if security == "reality" and not pbk:
            return None
        return {"type": "vless", "server": host, "port": port, "uuid": unquote(u.username or ""),
                "flow": q("flow"), "security": security, "tls": security in ("tls", "reality"),
                "reality": security == "reality", "reality_pbk": pbk, "reality_sid": sid,
                "fingerprint": q("fp", "fingerprint", default="chrome"), "network": net,
                "path": unquote(q("path", default="/")), "host_header": q("host"),
                "sni": q("sni", "serverName"), "name": name}

    if proto == "trojan":
        if net not in ("ws", "tcp"):
            return None
        security = q("security", default="tls").lower()
        if security not in ("tls", "reality"):
            return None                         # QX Trojan requires TLS (or WSS/Reality)
        pbk, sid = q("pbk", "publicKey"), q("sid", "shortId")
        if security == "reality" and not pbk:
            return None
        return {"type": "trojan", "server": host, "port": port,
                "password": unquote(u.username or "") + ((":" + unquote(u.password)) if u.password else ""),
                "security": security, "tls": security in ("tls", "reality"),
                "reality": security == "reality", "reality_pbk": pbk, "reality_sid": sid,
                "fingerprint": q("fp", "fingerprint", default="chrome"), "network": net,
                "path": unquote(q("path", default="/")), "host_header": q("host"),
                "sni": q("sni", "serverName"), "name": name}

    if proto == "ss":
        if q("plugin"):
            return None                       # never silently strip a required plugin
        try:
            # SIP002 modern form: base64(method:password)@host:port
            if "@" in u.netloc:
                cred = unquote(u.username or "")
                if u.password is not None:
                    method, pwd = unquote(u.username or ""), unquote(u.password)
                else:
                    method, sep, pwd = b64decode_any(cred).partition(":")
            else:
                # legacy form: base64(method:password@host:port)
                whole = b64decode_any(raw[5:].split("#", 1)[0].split("?", 1)[0])
                cred, sep, _address = whole.rpartition("@")
                method, sep2, pwd = cred.partition(":")
                if not sep or not sep2:
                    return None
        except Exception:
            return None
        if method.lower() not in SS_CIPHERS_OK:
            return None
        return {"type": "ss", "server": host, "port": port, "cipher": method.lower(),
                "password": pwd, "network": "tcp", "tls": False, "security": "none", "name": name}
    return None


# —— 地区判定 ——
# —— 地区判定 ——
def region_by_name(name):
    n = " " + (name or "").lower() + " "
    for code in ("HK", "TW", "JP", "US"):
        for k in REGION_KEYS[code]:
            if k in n:
                return code
    return ""


def geo_batch(ips, cache_file, max_ips=2000):
    """IP 归属地兜底（ip-api 批量，带本地缓存，失败就静默跳过）"""
    cache = {}
    if os.path.exists(cache_file):
        try:
            cache = json.load(open(cache_file, encoding="utf-8"))
        except Exception:
            cache = {}
    todo = [ip for ip in ips if ip not in cache][:max_ips]
    got = 0
    for i in range(0, len(todo), 100):
        chunk = todo[i:i + 100]
        try:
            body = json.dumps([{"query": ip, "fields": "query,countryCode"} for ip in chunk]).encode()
            req = urllib.request.Request("http://ip-api.com/batch", data=body,
                                         headers={"User-Agent": UA, "Content-Type": "application/json"})
            res = json.loads(urllib.request.urlopen(req, timeout=20).read().decode("utf-8", "ignore"))
            for item in res:
                if item.get("query"):
                    cache[item["query"]] = (item.get("countryCode") or "").upper()
                    got += 1
        except Exception:
            break
        time.sleep(1.2)
    try:
        json.dump(cache, open(cache_file, "w", encoding="utf-8"))
    except Exception:
        pass
    return cache, got


# —— 出口方式：直连 或 经上游 HTTP 代理（本机无直连出口时用）——
def detect_proxy(spec):
    """spec: off / auto / http://host:port"""
    if not spec or spec == "off":
        return None
    if spec != "auto":
        from urllib.parse import urlparse
        u = urlparse(spec if "://" in spec else "http://" + spec)
        return (u.hostname, u.port or 8080) if u.hostname else None
    px = urllib.request.getproxies()
    for k in ("https", "http", "all"):
        v = px.get(k)
        if v and v.startswith("http"):
            from urllib.parse import urlparse
            u = urlparse(v)
            if u.hostname:
                return (u.hostname, u.port or 8080)
    return None


class ConnectRelay:
    """本地 SOCKS5 → 上游 HTTP 代理 CONNECT → 目标。给 Xray 的 dialerProxy 用。"""

    def __init__(self, proxy_host, proxy_port):
        self.ph, self.pp = proxy_host, proxy_port
        self.srv = None
        self.port = 0
        self._stop = False
        self.stats = {"req": 0, "ok": 0, "rate": 0, "fail": 0}

    def start(self):
        self.srv = socket.socket()
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("127.0.0.1", 0))
        self.srv.listen(128)
        self.port = self.srv.getsockname()[1]
        import threading
        threading.Thread(target=self._loop, daemon=True).start()
        return self.port

    def _loop(self):
        while not self._stop:
            try:
                c, _ = self.srv.accept()
            except Exception:
                break
            import threading
            threading.Thread(target=self._handle, args=(c,), daemon=True).start()

    def _handle(self, c):
        up = None
        try:
            c.settimeout(15)
            d = c.recv(262)
            if not d or d[0] != 5:
                return
            c.sendall(b"\x05\x00")
            req = c.recv(4)
            if len(req) < 4:
                return
            atyp = req[3]
            if atyp == 1:
                host = socket.inet_ntoa(c.recv(4))
            elif atyp == 3:
                ln = c.recv(1)[0]
                host = c.recv(ln).decode("utf-8", "ignore")
            elif atyp == 4:
                host = socket.inet_ntop(socket.AF_INET6, c.recv(16))
            else:
                return
            port = int.from_bytes(c.recv(2), "big")
            up = socket.create_connection((self.ph, self.pp), timeout=10)
            up.sendall(("CONNECT %s:%d HTTP/1.1\r\nHost: %s:%d\r\n"
                        "Proxy-Connection: keep-alive\r\n\r\n" % (host, port, host, port)).encode())
            buf = b""
            while b"\r\n\r\n" not in buf:
                ch = up.recv(256)
                if not ch:
                    break
                buf += ch
            if b" 200 " not in buf.split(b"\r\n")[0]:
                self.stats["req"] += 1
                if b" 429 " in buf or b"429" in buf.split(b"\r\n")[0]:
                    self.stats["rate"] += 1
                else:
                    self.stats["fail"] += 1
                c.sendall(b"\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00")
                return
            self.stats["req"] += 1
            self.stats["ok"] += 1
            c.sendall(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
            self._pipe(c, up)
        except Exception:
            pass
        finally:
            for s in (c, up):
                try:
                    if s:
                        s.close()
                except Exception:
                    pass

    @staticmethod
    def _pipe(a, b):
        import threading, select
        socks = [a, b]
        try:
            a.settimeout(None)
            b.settimeout(None)
        except Exception:
            return
        while True:
            try:
                r, _, _ = select.select(socks, [], [], 20)
            except Exception:
                return
            if not r:
                return
            for s in r:
                try:
                    d = s.recv(65536)
                except Exception:
                    return
                if not d:
                    return
                try:
                    (b if s is a else a).sendall(d)
                except Exception:
                    return

    def stop(self):
        self._stop = True
        try:
            self.srv.close()
        except Exception:
            pass


# —— TCP RTT 初筛 ——
RL = {"until": 0.0, "hits": 0}


def _rl_backoff():
    RL["hits"] += 1
    RL["until"] = max(RL["until"], time.monotonic() + 2.0 + min(RL["hits"] * 0.15, 12))


async def _rl_wait():
    now = time.monotonic()
    if RL["until"] > now:
        await asyncio.sleep(min(RL["until"] - now, 5))


async def tcp_rtt(host, port, rounds, timeout, sem, via=None):
    async with sem:
        best = None
        for _ in range(rounds):
            if via:
                await _rl_wait()
            t0 = time.perf_counter()
            w = None
            try:
                if via:
                    r, w = await asyncio.wait_for(asyncio.open_connection(via[0], via[1]), timeout=timeout)
                    w.write(("CONNECT %s:%d HTTP/1.1\r\nHost: %s:%d\r\n\r\n" % (host, port, host, port)).encode())
                    await asyncio.wait_for(w.drain(), timeout=timeout)
                    buf = b""
                    while b"\r\n\r\n" not in buf:
                        ch = await asyncio.wait_for(r.read(256), timeout=timeout)
                        if not ch:
                            break
                        buf += ch
                    head = buf.split(b"\r\n")[0]
                    if b"429" in head:
                        _rl_backoff()
                        return None
                    if b" 200 " not in head:
                        raise OSError("proxy refused")
                else:
                    r, w = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
                dt = (time.perf_counter() - t0) * 1000
                best = dt if best is None else min(best, dt)
            except Exception:
                pass
            finally:
                if w is not None:
                    try:
                        w.close()
                    except Exception:
                        pass
        return best


async def tcp_screen(nodes, concurrency, rounds, timeout, via=None):
    sem = asyncio.Semaphore(concurrency)
    tasks = [tcp_rtt(n["server"], n["port"], rounds, timeout, sem, via) for n in nodes]
    res = await asyncio.gather(*tasks)
    out = []
    for n, rtt in zip(nodes, res):
        if rtt is not None:
            n["tcp_ms"] = round(rtt, 1)
            out.append(n)
    return out


async def tcp_screen_adaptive(nodes, concurrency, rounds, timeout, via, target, budget):
    """分批推进：攒够 target 个可达节点就停，省时间也少给出口添压力"""
    sem = asyncio.Semaphore(concurrency)
    passes, pending = [], list(nodes)
    wave = max(concurrency * 2, 48)
    t0 = time.time()
    while pending and len(passes) < target:
        if budget and time.time() - t0 > budget:
            break
        batch, pending = pending[:wave], pending[wave:]
        res = await asyncio.gather(*[tcp_rtt(n["server"], n["port"], rounds, timeout, sem, via) for n in batch])
        for n, rtt in zip(batch, res):
            if rtt is not None:
                n["tcp_ms"] = round(rtt, 1)
                passes.append(n)
    return passes, len(nodes) - len(pending)


# —— Xray 代理层实测 ——
def ensure_xray(path=None):
    if path and os.path.exists(path):
        return path
    for cand in (shutil.which("xray"), os.path.join(os.getcwd(), "bin", "xray"),
                 os.path.expanduser("~/.local/bin/xray"), os.path.join(os.getcwd(), "xray")):
        if cand and os.path.exists(cand):
            return cand
    bindir = os.path.join(os.getcwd(), "bin")
    os.makedirs(bindir, exist_ok=True)
    zpath = os.path.join(bindir, "xray.zip")
    asset = xray_asset()
    for url in [u % asset for u in XRAY_URLS]:
        got = False
        for tool in (["curl", "-sL", "--max-time", "180", "-o", zpath, url],
                     ["wget", "-q", "-O", zpath, url]):
            try:
                if shutil.which(tool[0]) and subprocess.run(tool, timeout=200).returncode == 0 \
                        and os.path.exists(zpath) and os.path.getsize(zpath) > 1000000:
                    got = True
                    break
            except Exception:
                continue
        if not got:
            continue
        try:
            import zipfile
            with zipfile.ZipFile(zpath) as z:
                z.extractall(bindir)
            for nm in ("xray", "xray.exe"):
                fp = os.path.join(bindir, nm)
                if os.path.exists(fp):
                    os.chmod(fp, 0o755)
                    os.remove(zpath)
                    return fp
        except Exception:
            continue
    return None


def build_outbound(n):
    t = n["type"]
    ss = {"address": n["server"], "port": n["port"]}
    stream = {"network": n.get("network", "tcp")}
    sni = n.get("sni") or n.get("host_header") or n["server"]
    if n.get("reality"):
        stream["security"] = "reality"
        stream["realitySettings"] = {"serverName": sni, "publicKey": n["reality_pbk"],
                                     "shortId": n.get("reality_sid") or "",
                                     "fingerprint": n.get("fingerprint") or "chrome"}
    elif n.get("tls"):
        stream["security"] = "tls"
        stream["tlsSettings"] = {"allowInsecure": True, "serverName": sni}
    if n.get("network") == "ws":
        stream["wsSettings"] = {"path": n.get("path") or "/",
                                "headers": {"Host": n.get("host_header") or n.get("sni") or n["server"]}}
    elif n.get("network") == "grpc":
        stream["grpcSettings"] = {"serviceName": n.get("service") or n.get("path") or ""}

    if t == "ss":
        return {"protocol": "shadowsocks",
                "settings": {"servers": [{"address": n["server"], "port": n["port"],
                                          "method": n["cipher"], "password": n["password"]}]},
                "streamSettings": {"network": "tcp"}}
    if t == "vmess":
        qx_cipher = VMESS_CIPHER_QX.get((n.get("cipher") or "auto").lower())
        user = {"id": n["uuid"], "alterId": n.get("aid", 0), "security": qx_cipher or "auto"}
        return {"protocol": "vmess",
                "settings": {"vnext": [{"address": n["server"], "port": n["port"], "users": [user]}]},
                "streamSettings": stream}
    if t == "vless":
        user = {"id": n["uuid"], "encryption": "none"}
        if n.get("flow"):
            user["flow"] = n["flow"]
        return {"protocol": "vless",
                "settings": {"vnext": [{"address": n["server"], "port": n["port"], "users": [user]}]},
                "streamSettings": stream}
    if t == "trojan":
        return {"protocol": "trojan",
                "settings": {"servers": [{"address": n["server"], "port": n["port"], "password": n["password"]}]},
                "streamSettings": stream}
    return None


def validate_configs(nodes, xray_bin, workers=8):
    """逐条跑一次配置自检：核心加载不了的节点直接剔除，不然会拖垮整批"""
    def check(n):
        ob = build_outbound(n)
        if not ob:
            return None
        cfg = {"inbounds": [{"port": 1080, "protocol": "socks", "settings": {"udp": False}}],
               "outbounds": [ob]}
        fd, path = tempfile.mkstemp(suffix=".json", prefix="qxchk_")
        with os.fdopen(fd, "w") as f:
            json.dump(cfg, f)
        try:
            r = subprocess.run([xray_bin, "run", "-test", "-c", path],
                               capture_output=True, timeout=15)
            return n if r.returncode == 0 else None
        except Exception:
            return None
        finally:
            try:
                os.remove(path)
            except Exception:
                pass

    out = []
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        for r in ex.map(check, nodes):
            if r is not None:
                out.append(r)
    return out


def run_xray(cfg_path):
    return subprocess.Popen([XRAY_BIN, "run", "-c", cfg_path],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def proxy_delay_test(nodes, xray_bin, chunk=24, timeout=5, workers=24, relay=None, relay_ctl=None, rounds=2):
    """每批起一个 Xray；按 QX 的 HEAD 方式复测，全部轮次成功才算可用。"""
    global XRAY_BIN
    XRAY_BIN = xray_bin
    results = []
    total = len(nodes)
    for ci in range(0, total, chunk):
        batch = nodes[ci:ci + chunk]
        inbounds, outbounds, rules = [], [], []
        port_map = {}
        base_port = 20000 + random.randint(0, 200) * 100
        for i, n in enumerate(batch):
            ob = build_outbound(n)
            if not ob:
                continue
            if relay:
                ob.setdefault("streamSettings", {})["sockopt"] = {"dialerProxy": "relay"}
            p = base_port + i
            port_map[p] = n
            inbounds.append({"tag": "in%d" % i, "listen": "127.0.0.1", "port": p,
                             "protocol": "socks", "settings": {"udp": False, "auth": "noauth"}})
            ob["tag"] = "out%d" % i
            outbounds.append(ob)
            rules.append({"type": "field", "inboundTag": ["in%d" % i], "outboundTag": "out%d" % i})
        if not port_map:
            continue
        outbounds.append({"protocol": "freedom", "tag": "direct"})
        if relay:
            outbounds.append({"protocol": "socks", "tag": "relay",
                              "settings": {"servers": [{"address": "127.0.0.1", "port": relay}]}})
        cfg = {"log": {"loglevel": "none"}, "inbounds": inbounds, "outbounds": outbounds,
               "routing": {"domainStrategy": "AsIs", "rules": rules}}
        fd, path = tempfile.mkstemp(suffix=".json", prefix="qxsel_")
        with os.fdopen(fd, "w") as f:
            json.dump(cfg, f)
        proc = run_xray(path)
        time.sleep(1.1)

        def probe(item):
            p, n = item
            delays = []
            try:
                for _ in range(max(1, rounds)):
                    r = subprocess.run(["curl", "-I", "-s", "-o", "/dev/null", "-w", "%{http_code} %{time_total}",
                                        "-x", "socks5h://127.0.0.1:%d" % p, "--max-time", str(timeout),
                                        "http://www.gstatic.com/generate_204"],
                                       capture_output=True, text=True, timeout=timeout + 3)
                    code, _, elapsed = r.stdout.strip().partition(" ")
                    if code != "204":
                        return n, None
                    delays.append(float(elapsed) * 1000)
                return n, round(sum(delays) / len(delays), 1)
            except Exception:
                return n, None

        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            for n, delay in ex.map(probe, list(port_map.items())):
                if delay is not None:
                    n["delay_ms"] = delay
                    results.append(n)
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except Exception:
            proc.kill()
        os.remove(path)
        log("  代理层实测 %d/%d，已确认可用 %d 条" % (min(ci + chunk, total), total, len(results)))
        if relay_ctl and relay_ctl.stats["req"] > 120 and \
                relay_ctl.stats["rate"] / relay_ctl.stats["req"] > 0.35:
            log("  !! 出口限速严重，提前收手，用已测出的结果")
            break
    return results


# —— QX 格式输出 ——
VMESS_CIPHER_QX = {
    "auto": "chacha20-poly1305", "none": "none", "zero": None,
    "aes-128-gcm": "aes-128-gcm", "chacha20-poly1305": "chacha20-poly1305",
    "chacha20-ietf-poly1305": "chacha20-poly1305",
}


def qx_safe(value):
    value = str(value)
    return value if value and not re.search(r"[,\r\n]", value) else None


def to_qx_line(n):
    """Render the exact protocol semantics accepted by current QX sample.conf."""
    t, host, port = n["type"], n["server"], n["port"]
    tag = clean_name(n.get("tag") or n.get("name")) or ("%s-%s" % (t, host))
    net = (n.get("network") or "tcp").lower()
    if net not in ("tcp", "ws"):
        return None
    cred = n.get("password") if t in ("ss", "trojan") else n.get("uuid")
    if not qx_safe(cred):
        return None
    if t == "ss":
        p = ["shadowsocks=%s:%d" % (host, port), "method=" + n["cipher"], "password=" + str(cred)]
    elif t == "vmess":
        c = VMESS_CIPHER_QX.get((n.get("cipher") or "auto").lower())
        if not c: return None
        p = ["vmess=%s:%d" % (host, port), "method=" + c, "password=" + str(cred)]
        if int(n.get("aid") or 0) > 0: p.append("aead=false")
    elif t == "vless":
        p = ["vless=%s:%d" % (host, port), "method=none", "password=" + str(cred)]
        if n.get("flow"): p.append("vless-flow=" + n["flow"])
    elif t == "trojan":
        p = ["trojan=%s:%d" % (host, port), "password=" + str(cred)]
    else:
        return None

    tls, reality = bool(n.get("tls")), bool(n.get("reality"))
    sni = n.get("sni") or n.get("host_header") or host
    path = n.get("path") or "/"
    hhost = n.get("host_header") or n.get("sni") or host
    if not all(qx_safe(x) for x in (sni, path, hhost)):
        return None
    if net == "ws":
        p += ["obfs=" + ("wss" if tls else "ws"), "obfs-uri=" + path, "obfs-host=" + hhost]
        if tls and not reality: p.append("tls-verification=false")
    elif tls and t == "trojan":
        p += ["over-tls=true", "tls-host=" + sni]
        if not reality: p.append("tls-verification=false")
    elif tls:
        p += ["obfs=over-tls", "obfs-host=" + sni]
        if not reality: p.append("tls-verification=false")
    if reality:
        p.append("reality-base64-pubkey=" + n["reality_pbk"])
        if n.get("reality_sid"): p.append("reality-hex-shortid=" + n["reality_sid"])
    p += ["udp-relay=true", "server_check_url=http://www.gstatic.com/generate_204", "tag=" + tag]
    return ", ".join(p)


# —— 主流程 ——
# —— 固定直链发布 ——
def publish_github(file_path, repo, branch="main", remote_path="qx_sub.txt"):
    """通过 GitHub Contents API 原子更新公开订阅；令牌只从环境或 gh 登录读取。"""
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token and shutil.which("gh"):
        try:
            token = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True,
                                   timeout=15, check=True).stdout.strip()
        except Exception:
            token = ""
    if not token:
        raise RuntimeError("发布需要 GH_TOKEN/GITHUB_TOKEN，或先执行 gh auth login")
    from urllib.parse import quote
    api = "https://api.github.com/repos/%s/contents/%s" % (repo, quote(remote_path))
    headers = {"Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
               "X-GitHub-Api-Version": "2022-11-28", "User-Agent": UA}
    sha = None
    try:
        req = urllib.request.Request(api + "?ref=" + quote(branch), headers=headers)
        sha = json.loads(urllib.request.urlopen(req, timeout=30).read()).get("sha")
    except Exception as e:
        if getattr(e, "code", None) != 404:
            raise
    payload = {"message": "Update verified QX nodes " + time.strftime("%Y-%m-%d %H:%M:%S"),
               "content": base64.b64encode(open(file_path, "rb").read()).decode(), "branch": branch}
    if sha:
        payload["sha"] = sha
    req = urllib.request.Request(api, data=json.dumps(payload).encode(), headers=headers, method="PUT")
    urllib.request.urlopen(req, timeout=45).read()
    return "https://raw.githubusercontent.com/%s/%s/%s" % (repo, branch, remote_path)


# —— 主流程 ——
def collect_sources(args):
    sources = dict(DEFAULT_SOURCES)
    for fn in ("sources.txt", args.sources) if args.sources else ("sources.txt",):
        if fn and os.path.exists(fn):
            for i, line in enumerate(open(fn, encoding="utf-8")):
                line = line.strip()
                if line and not line.startswith("#"):
                    sources["user%d" % i] = line
    return sources


def run_once(args):
    t0 = time.time()
    os.makedirs(args.out, exist_ok=True)

    # 1) 抓取
    sources = collect_sources(args)
    log("① 抓取公开节点源（%d 个）…" % len(sources))
    links, src_meta = {}, {}
    for key, url in sources.items():
        txt, err = fetch(url)
        if err:
            log("   - %-14s 失败(%s)" % (key, err[:40]))
            continue
        got = extract(txt)
        if "proxies" in txt and len(got) < 50:
            got += extract_from_yaml(txt)
        cnt = 0
        for lk in got:
            lk = lk.strip()
            if lk not in links:
                links[lk] = key
                cnt += 1
        log("   - %-14s +%d" % (key, cnt))
    log("   共 %d 条原始节点" % len(links))

    # 2) 解析 + 去重
    log("② 解析协议、去重…")
    nodes, seen = [], set()
    for lk, src in links.items():
        try:
            p = parse_node(lk)
            if not p or p["proto"] == "ssr":
                continue
            name = p["name"]
            if JUNK.search(name):
                continue
            mid = to_mid(p["proto"], p)
        except Exception:
            continue
        if not mid:
            continue
        if mid["type"] in ("vmess", "vless") and not mid.get("uuid"):
            continue
        if mid["type"] in ("ss", "trojan") and not mid.get("password"):
            continue
        if mid["type"] == "ss" and mid.get("cipher", "").lower() not in SS_CIPHERS_OK:
            continue
        key = json.dumps({k: mid.get(k) for k in ("type", "server", "port", "uuid", "password", "cipher",
                    "aid", "network", "security", "path", "host_header", "sni", "flow",
                    "reality_pbk", "reality_sid")}, sort_keys=True, ensure_ascii=True)
        if key in seen or not to_qx_line(mid):
            continue
        seen.add(key)
        mid["source"] = src
        mid["raw"] = lk
        nodes.append(mid)
    log("   有效节点 %d 条" % len(nodes))

    # 3) 地区归类
    log("③ 地区归类（港/TW/日/美）…")
    requested = {r.strip().upper() for r in args.regions.split(",") if r.strip()}
    want = [r for r in ("HK", "TW", "JP", "US") if r in requested]
    buckets = {r: [] for r in want}
    unknown = []
    for n in nodes:
        r = region_by_name(n.get("name", ""))
        if not r:
            r = region_by_name(n["server"])
        if r in buckets:
            n["region"] = r
            buckets[r].append(n)
        else:
            unknown.append(n)
    log("   按名字命中：%s" % "、".join("%s %d" % (r, len(buckets[r])) for r in want))

    if args.geo and unknown:
        ips = []
        for n in unknown:
            if re.fullmatch(r'\d+\.\d+\.\d+\.\d+', n["server"]) and n["server"] not in ips:
                ips.append(n["server"])
        cache, got = geo_batch(ips, os.path.join(args.out, "geo_cache.json"), args.geo_max)
        if got:
            log("   另有 %d 条按 IP 归属地补判…" % got)
        for n in unknown:
            cc = cache.get(n["server"], "")
            if cc in buckets:
                n["region"] = cc
                buckets[cc].append(n)
    for r in want:
        log("   %s：候选 %d 条" % (r, len(buckets[r])))

    cand = [n for r in want for n in buckets[r]]
    if not cand:
        log("!! 没有候选节点，看看源是不是都失效了")
        return None

    # 4) 抽样：每地区限量，避免把出口打到限速
    via = detect_proxy(args.via_proxy)
    conc = args.tcp_concurrency if not via else min(args.tcp_concurrency, 48)
    RL["hits"] = 0
    sample = []
    for r in want:
        lst = buckets[r][:]
        random.shuffle(lst)
        take_n = args.region_candidates or len(lst)
        sample += lst[:take_n]
        log("   %s：候选 %d 条 → 取 %d 条进测速" % (r, len(lst), min(take_n, len(lst))))

    # 5) 测速：优先用真实代理层复测（一次搞定"能不能用"和"多快"）
    xray_bin = None if args.tcp_only else ensure_xray(args.xray)
    relay = None
    if xray_bin:
        rport = None
        if via:
            relay = ConnectRelay(via[0], via[1])
            rport = relay.start()
        try:
            before = len(sample)
            sample = validate_configs(sample, xray_bin)
            if before != len(sample):
                log("   配置自检：%d 条可测（剔除 %d 条核心不兼容的）" % (len(sample), before - len(sample)))
            log("⑤ 代理层真实延迟复测（%d 条）…" % len(sample))
            verified = proxy_delay_test(sample, xray_bin, chunk=args.proxy_chunk,
                                        timeout=args.delay_timeout, workers=args.proxy_chunk,
                                        relay=rport, relay_ctl=relay, rounds=args.verify_rounds)
        finally:
            if relay:
                st = dict(relay.stats)
                relay.stop()
                if st["req"] and st["rate"] / max(st["req"], 1) > 0.15:
                    log("   出口限速命中 %d/%d，结果可能偏少" % (st["rate"], st["req"]))
        if not verified:
            if args.allow_tcp_fallback:
                log("   !! 代理验证为 0；按显式选项退回仅 TCP 探测（结果不算已验证）")
                xray_bin = None
            else:
                log("!! 代理层没有验证出可用节点；为避免假活，本轮不输出。可用 --allow-tcp-fallback 调试")
                return None
    if not xray_bin:
        log("④ TCP 握手初筛（并发 %d%s）…" % (conc, "，经上游代理隧道" if via else ""))
        verified = asyncio.run(tcp_screen(sample, conc, args.tcp_rounds, args.tcp_timeout, via))
        for n in verified:
            n["delay_ms"] = n["tcp_ms"]
            n["verification"] = "tcp-only-unverified"
        if via and RL["hits"]:
            log("   触发出口限速 %d 次，已自动退避" % RL["hits"])
    if not verified:
        log("!! 一条可用节点都没测出来，源可能全挂了")
        return None

    if xray_bin:
        for n in verified:
            n["verification"] = "xray-http-204"

    # 5.5) 跨轮累积：实测通过的节点存进缓存，多跑几轮越攒越多
    def nkey(n):
        return json.dumps({k: n.get(k) for k in ("type", "server", "port", "uuid", "password", "cipher",
                    "aid", "network", "security", "path", "host_header", "sni", "flow",
                    "reality_pbk", "reality_sid")}, sort_keys=True, ensure_ascii=True)

    cache_path = os.path.join(args.out, "verified_cache.json")
    cache = {}
    if os.path.exists(cache_path):
        try:
            cache = json.load(open(cache_path, encoding="utf-8"))
        except Exception:
            cache = {}
    now = time.time()
    for n in verified:
        if n.get("verification") != "xray-http-204":
            continue
        n["seen"] = now
        n["cache_schema"] = 2
        cache[nkey(n)] = n
    keep = {}
    for k, v in cache.items():
        if not isinstance(v, dict) or v.get("verification") != "xray-http-204" or v.get("cache_schema") != 2:
            continue
        if args.cache_ttl and now - v.get("seen", 0) > args.cache_ttl * 3600:
            continue
        keep[k] = v
    json.dump(keep, open(cache_path, "w", encoding="utf-8"), ensure_ascii=False)
    fresh = set(nkey(n) for n in verified)
    merged = list(verified) if not xray_bin else list(verified) + [dict(v) for k, v in keep.items() if k not in fresh]
    log("   历史累积可用节点 %d 条（本轮新测出 %d 条）" % (len(keep), len(verified)))

    # 6) 每地区取最快 + 子网去重
    log("⑥ 优选（每地区上限 %d，总数上限 %d，同段最多 %d）…"
        % (args.per_region, args.total, args.max_per_subnet))
    picked, sub_count = [], {}
    pool = {r: sorted([n for n in merged if n.get("region") == r and n.get("delay_ms") is not None
                       and n["delay_ms"] <= args.max_delay], key=lambda x: x["delay_ms"]) for r in want}
    dropped = len([n for n in merged if n.get("delay_ms") and n["delay_ms"] > args.max_delay])
    if dropped:
        log("   延迟超过 %.0fms 的 %d 条已剔除" % (args.max_delay, dropped))

    def take(r, quota):
        got = 0
        for n in pool[r]:
            if n in picked:
                continue
            m = re.match(r'(\d+)\.(\d+)\.(\d+)\.', n["server"])
            sub = "%s.%s.%s" % m.groups() if m else n["server"]
            if sub_count.get(sub, 0) >= args.max_per_subnet:
                continue
            sub_count[sub] = sub_count.get(sub, 0) + 1
            picked.append(n)
            got += 1
            if got >= quota or len(picked) >= args.total:
                break
        return got

    deficit = 0
    for r in want:
        quota = min(args.per_region, args.total - len(picked))
        got = take(r, quota)
        deficit += max(0, args.per_region - got)
        if len(picked) >= args.total:
            break
    if len(picked) < args.total and deficit:
        for r in sorted(want, key=lambda x: len(pool[x]), reverse=True):
            if len(picked) >= args.total:
                break
            take(r, args.total - len(picked))

    picked.sort(key=lambda x: (want.index(x["region"]), x["delay_ms"]))
    for r in want:
        got = [n for n in picked if n["region"] == r]
        if got:
            log("   %s：%d 条，最快 %.0fms / 最慢 %.0fms"
                % (r, len(got), got[0]["delay_ms"], got[-1]["delay_ms"]))

    # 7) 输出
    log("⑦ 生成 QX 订阅与产物…")
    cn = {"HK": "香港", "TW": "台湾", "JP": "日本", "US": "美国"}
    for i, n in enumerate(picked, 1):
        brief = clean_name(n.get("name"))[:22].strip()
        n["tag"] = "%s%02d %s%dms %s" % (n["region"], i, cn.get(n["region"], ""), int(n["delay_ms"]), brief)
        n["tag"] = re.sub(r'\s+', " ", n["tag"]).strip()[:58]

    rendered = [(n, to_qx_line(n)) for n in picked]
    picked = [n for n, line in rendered if line]
    qx_lines = [line for n, line in rendered if line]
    qx_txt = "\n".join(qx_lines) + "\n"
    with open(os.path.join(args.out, "qx_sub.txt"), "w", encoding="utf-8") as f:
        f.write(qx_txt)
    with open(os.path.join(args.out, "qx_sub_base64.txt"), "w", encoding="utf-8") as f:
        f.write(base64.b64encode(qx_txt.encode()).decode())
    with open(os.path.join(args.out, "nodes_best.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(n["raw"] for n in picked) + "\n")
    json.dump([{k: v for k, v in n.items()} for n in picked],
              open(os.path.join(args.out, "selection.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)

    rep = ["QX 节点优选报告 — %s" % time.strftime("%Y-%m-%d %H:%M:%S"),
           "耗时 %.1f 秒" % (time.time() - t0),
           "公开源原始节点 %d → 解析有效 %d → 取样测速 %d → 实测可用 %d"
           % (len(links), len(nodes), len(sample), len(verified)),
           "选中 %d 条（%s）" % (len(picked), "、".join("%s %d" % (r, sum(1 for n in picked if n["region"] == r)) for r in want)),
           "其中本轮实测 %d 条，历史累积 %d 条" % (len(verified), len(picked) - len([n for n in picked if n in verified])),
           ""]
    for n in picked:
        rep.append("[%s] %6.1fms  %-9s %s:%d  %s" % (n["region"], n["delay_ms"], n["type"],
                                                      n["server"], n["port"], n["tag"]))
    open(os.path.join(args.out, "report.txt"), "w", encoding="utf-8").write("\n".join(rep) + "\n")

    log("完成：%s" % os.path.join(args.out, "qx_sub.txt"))
    return picked


def main():
    ap = argparse.ArgumentParser(description="公开节点池 → 港/TW/日/美 延迟优选 → QX 订阅")
    ap.add_argument("--regions", default="HK,TW,JP,US", help="目标地区，逗号分隔")
    ap.add_argument("--total", type=int, default=100, help="节点总数上限，默认 100")
    ap.add_argument("--per-region", type=int, default=25, help="每个地区上限，默认 25")
    ap.add_argument("--max-per-subnet", type=int, default=3, help="同一 /24 网段最多取几条，默认 3")
    ap.add_argument("--tcp-timeout", type=float, default=4.0)
    ap.add_argument("--tcp-rounds", type=int, default=2)
    ap.add_argument("--tcp-concurrency", type=int, default=200)
    ap.add_argument("--tcp-pass-target", type=int, default=80, help="已弃用（保留兼容）")
    ap.add_argument("--tcp-budget", type=float, default=200, help="已弃用（保留兼容）")
    ap.add_argument("--region-candidates", type=int, default=300,
                    help="每个地区最多取多少条进测速（0 = 不限），默认 300")
    ap.add_argument("--proxy-candidates", type=int, default=90, help="已弃用（保留兼容）")
    ap.add_argument("--proxy-chunk", type=int, default=24, help="复测并发批大小")
    ap.add_argument("--delay-timeout", type=float, default=5)
    ap.add_argument("--verify-rounds", type=int, default=2,
                    help="代理层连续通过轮数，默认 2；每轮均须 HTTP 204")
    ap.add_argument("--tcp-only", action="store_true", help="仅 TCP 端口探测；结果未经代理协议验证")
    ap.add_argument("--allow-tcp-fallback", action="store_true",
                    help="Xray 验证为 0 时仍输出仅 TCP 可达节点（不推荐，默认关闭）")
    ap.add_argument("--no-geo", dest="geo", action="store_false", help="禁用 IP 归属地兜底")
    ap.add_argument("--geo-max", type=int, default=2000, help="最多查多少个 IP 的归属地")
    ap.add_argument("--max-delay", type=float, default=3000,
                    help="延迟超过这个毫秒数的节点直接不要，默认 3000")
    ap.add_argument("--cache-ttl", type=float, default=12,
                    help="实测可用节点的缓存有效小时数（默认 12，0 = 永不过期）")
    ap.add_argument("--via-proxy", default="off",
                    help="出口方式：off 直连（默认）/ auto 自动用系统代理 / http://host:port 指定代理")
    ap.add_argument("--xray", default="", help="Xray 核心路径（默认自动找/下载）")
    ap.add_argument("--sources", default="", help="额外的源文件（一行一个 URL）")
    ap.add_argument("--out", default="out_qx")
    ap.add_argument("--interval", type=int, default=0, help="常驻模式，N 秒一轮")
    ap.add_argument("--upload", default="", help="跑完自动 SCP 推送，格式 用户@主机:/目标目录/")
    ap.add_argument("--publish-github", default="art61771-stack/qx-node-subscription",
                    help="成功后发布到 GitHub 仓库；默认已配置固定公开仓库，填 off 可关闭")
    ap.add_argument("--publish-branch", default="main", help="GitHub 发布分支，默认 main")
    args = ap.parse_args()
    while True:
        try:
            picked = run_once(args)
            if picked and args.publish_github.lower() != "off":
                url = publish_github(os.path.join(args.out, "qx_sub.txt"),
                                     args.publish_github, args.publish_branch)
                log("QX 固定订阅链接：%s" % url)
                open(os.path.join(args.out, "subscription_url.txt"), "w", encoding="utf-8").write(url + "\n")
            if picked and args.upload:
                files = [os.path.join(args.out, x) for x in ("qx_sub.txt", "report.txt")]
                subprocess.run(["scp", "-O"] + files + [args.upload], check=False)
                log("已推送到 %s" % args.upload)
        except KeyboardInterrupt:
            break
        except Exception as e:
            log("本轮出错：%s" % e)
        if not args.interval:
            break
        log("休眠 %d 秒后开始下一轮…" % args.interval)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
