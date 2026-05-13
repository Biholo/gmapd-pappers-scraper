"""
Proxy service — local CONNECT tunnel + config resolution.

Usage:
    from services.proxy_service import ProxyService

    svc = ProxyService()
    if svc.enabled:
        svc.start()
        # opts.add_argument(f"--proxy-server=http://127.0.0.1:{svc.port}")
    svc.stop()

Env vars:
    PROXY_ENABLED   true (default) | false
    PROXY_LIST      http://u:p@host:port,... (comma-separated, priority)
    PROXY_HOST      host:port  (single proxy fallback)
    PROXY_USER      username
    PROXY_PASS      password
    PROXY_SESSION   true | false (default) — sticky session via hostname suffix
"""
import os
import socket
import base64
import hashlib
import logging
import threading
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class _LocalProxy:
    """Minimal CONNECT proxy on localhost:PORT — injects upstream auth. Zero deps."""

    def __init__(self, host: str, port: int, user: str, password: str):
        self._upstream = (host, port)
        self._auth = base64.b64encode(f"{user}:{password}".encode()).decode()
        self._server: socket.socket | None = None
        self.port: int = 0

    def start(self) -> int:
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind(("127.0.0.1", 0))
        self.port = self._server.getsockname()[1]
        self._server.listen(32)
        threading.Thread(target=self._accept_loop, daemon=True).start()
        return self.port

    def stop(self):
        try:
            self._server.close()
        except Exception:
            pass

    def _accept_loop(self):
        while True:
            try:
                client, _ = self._server.accept()
                threading.Thread(target=self._handle, args=(client,), daemon=True).start()
            except Exception:
                break

    @staticmethod
    def _relay(src: socket.socket, dst: socket.socket):
        try:
            while True:
                data = src.recv(8192)
                if not data:
                    break
                dst.sendall(data)
        except Exception:
            pass
        finally:
            for s in (src, dst):
                try:
                    s.close()
                except Exception:
                    pass

    def _handle(self, client: socket.socket):
        try:
            buf = b""
            while b"\r\n\r\n" not in buf:
                chunk = client.recv(4096)
                if not chunk:
                    return
                buf += chunk
            first_line = buf.split(b"\r\n")[0].decode(errors="replace")
            parts = first_line.split()
            if len(parts) < 2 or parts[0] != "CONNECT":
                client.close()
                return
            target = parts[1]
            upstream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            upstream.settimeout(30)
            upstream.connect(self._upstream)
            upstream.sendall((
                f"CONNECT {target} HTTP/1.1\r\n"
                f"Host: {target}\r\n"
                f"Proxy-Authorization: Basic {self._auth}\r\n\r\n"
            ).encode())
            resp = b""
            while b"\r\n\r\n" not in resp:
                chunk = upstream.recv(4096)
                if not chunk:
                    break
                resp += chunk
            if b"200" not in resp.split(b"\r\n")[0]:
                logger.warning("Upstream proxy rejected CONNECT: %s", resp.split(b"\r\n")[0])
                client.close()
                upstream.close()
                return
            client.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
            upstream.settimeout(None)
            threading.Thread(target=_LocalProxy._relay, args=(client, upstream), daemon=True).start()
            threading.Thread(target=_LocalProxy._relay, args=(upstream, client), daemon=True).start()
        except Exception as e:
            logger.debug("LocalProxy handle error: %s", e)
            try:
                client.close()
            except Exception:
                pass


class ProxyService:
    """Resolve proxy config from env and manage a LocalProxy tunnel."""

    def __init__(self):
        self._proxy: _LocalProxy | None = None
        self._upstream_url: str | None = self._resolve_url()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def enabled(self) -> bool:
        return self._upstream_url is not None

    @property
    def port(self) -> int:
        return self._proxy.port if self._proxy else 0

    def start(self) -> int:
        """Start local tunnel. Idempotent — safe to call multiple times."""
        if self._proxy is not None:
            return self._proxy.port
        if not self._upstream_url:
            return 0
        parsed = urlparse(self._upstream_url)
        self._proxy = _LocalProxy(
            parsed.hostname, parsed.port,
            parsed.username, parsed.password,
        )
        self._proxy.start()
        logger.info(
            "ProxyService started on 127.0.0.1:%d → %s:%d",
            self._proxy.port, parsed.hostname, parsed.port,
        )
        return self._proxy.port

    def stop(self):
        if self._proxy:
            self._proxy.stop()
            self._proxy = None

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_url() -> str | None:
        if os.getenv("PROXY_ENABLED", "true").strip().lower() in ("0", "false", "no"):
            return None

        # Priority 1: explicit list → pick by container hostname hash
        proxy_list_raw = os.getenv("PROXY_LIST", "").strip()
        if proxy_list_raw:
            proxies = [p.strip() for p in proxy_list_raw.split(",") if p.strip()]
            if proxies:
                idx = int(hashlib.md5(socket.gethostname().encode()).hexdigest(), 16) % len(proxies)
                url = proxies[idx]
                logger.info("ProxyService: list proxy %d/%d (host=%s)", idx + 1, len(proxies), socket.gethostname())
                return url

        # Priority 2: single host/user/pass
        host = os.getenv("PROXY_HOST", "").strip()
        user = os.getenv("PROXY_USER", "").strip()
        password = os.getenv("PROXY_PASS", "").strip()
        if not all([host, user, password]):
            return None

        use_session = os.getenv("PROXY_SESSION", "false").strip().lower() in ("1", "true", "yes")
        if use_session:
            session = socket.gethostname().replace("-", "").replace("_", "")[:8]
            full_user = f"{user}-session-{session}"
        else:
            full_user = user

        logger.info("ProxyService: http://%s:***@%s (session=%s)", full_user, host, use_session)
        return f"http://{full_user}:{password}@{host}"
