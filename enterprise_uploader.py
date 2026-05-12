"""
Enterprise Upload Engine v2 — Hospital-Grade Production
=========================================================
Fixes applied vs v1:
  [FIX 1] finalize_upload() — real HEAD + /verify MD5 check (was empty no-op)
  [FIX 2] SQLite WAL mode + SYNCHRONOUS FULL — crash-safe ledger
  [FIX 3] PermissionError / OSError / DiskFullError — EXE deployment safety
  [FIX 4] Proxy support (HTTPS_PROXY/HTTP_PROXY env), DNS failure handling,
           fallback API URL list
  [FIX 5] Immediate fsync-class flush on critical ledger writes
"""
from __future__ import annotations

import asyncio
import base64
import errno as _errno
import hashlib
import json
import logging
import os
import random
import shutil
import signal
import time
import uuid
import sqlite3
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin

import aiofiles
import httpx

# ==========================================
# 1. PRODUCTION CONFIGURATION
# ==========================================

def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)

def _env_int(key: str, default: int) -> int:
    return int(os.environ.get(key, default))

def _env_float(key: str, default: float) -> float:
    return float(os.environ.get(key, default))


# Add this import near your other imports at the top
from database import get_system_setting

# Keep your existing _env and _env_int functions, but add this new one below them:
def _db_or_env_int(key: str, default: int) -> int:
    """Checks the database first. If empty, falls back to .env or default."""
    # 1. Check Database
    db_val = get_system_setting(key, "")
    if db_val:
        try:
            return int(db_val)
        except ValueError:
            pass # If someone typed "abc" in the UI by accident, fall back to default
            
    # 2. Fallback to Environment
    return int(os.environ.get(key, default))

@dataclass(frozen=True)
class UploadConfig:
    min_chunk_size:       int   = field(default_factory=lambda: _env_int  ("UL_MIN_CHUNK",        512 * 1024))
    initial_chunk_size:   int   = field(default_factory=lambda: _db_or_env_int ("UL_CHUNK",          4 * 1024 * 1024))
    max_chunk_size:       int   = field(default_factory=lambda: _env_int  ("UL_MAX_CHUNK",      64 * 1024 * 1024))
    min_workers:          int   = field(default_factory=lambda: _env_int  ("UL_MIN_WORKERS",    1))
    initial_workers:      int   = field(default_factory=lambda: _db_or_env_int  ("UL_WORKERS",        4))
    max_workers:          int   = field(default_factory=lambda: _env_int  ("UL_MAX_WORKERS",    8))
    min_timeout:          float = field(default_factory=lambda: _env_float("UL_MIN_TIMEOUT",   10.0))
    initial_timeout:      float = field(default_factory=lambda: _env_float("UL_TIMEOUT",       30.0))
    max_timeout:          float = field(default_factory=lambda: _env_float("UL_MAX_TIMEOUT",  300.0))
    max_retries:          int   = field(default_factory=lambda: _db_or_env_int  ("UL_MAX_RETRIES",    7))
    rate_limit_bps:       int   = field(default_factory=lambda: _env_int  ("UL_RATE_LIMIT_BPS", 0))
    default_mode:         str   = field(default_factory=lambda: _env      ("UL_MODE",      "parallel"))
    net_check_host:       str   = field(default_factory=lambda: _env      ("UL_NET_HOST",   "8.8.8.8"))
    net_check_port:       int   = field(default_factory=lambda: _env_int  ("UL_NET_PORT",   53))
    net_check_interval:   float = field(default_factory=lambda: _env_float("UL_NET_INTERVAL", 5.0))
    net_wait_timeout:     float = field(default_factory=lambda: _env_float("UL_NET_WAIT",  300.0))
    speed_fast_threshold: int   = field(default_factory=lambda: _env_int  ("UL_SPEED_FAST", 10 * 1024 * 1024))
    speed_slow_threshold: int   = field(default_factory=lambda: _env_int  ("UL_SPEED_SLOW",      512 * 1024))
    scale_up_after:       int   = field(default_factory=lambda: _env_int  ("UL_SCALE_UP_AFTER", 10))
    # ── FIX 4: proxy + fallback URLs ──────────────────────────────────────
    # Comma-separated fallback URLs tried in order if primary fails
    # e.g. UL_FALLBACK_URLS="https://backup1.example.com/api/files/,https://backup2.example.com/api/files/"
    fallback_urls:        str   = field(default_factory=lambda: _env      ("UL_FALLBACK_URLS",  ""))
    proxy_url:            str   = field(default_factory=lambda: _env      ("UL_PROXY_URL",      ""))
    # FIX 1: whether to call /verify after upload (set False only for servers that don't support it)
    verify_after_upload:  bool  = field(default_factory=lambda: _env("UL_VERIFY_UPLOAD", "true").lower() == "true")
    # Min free disk MB required to start an upload (FIX 3)
    min_free_disk_mb:     int   = field(default_factory=lambda: _env_int  ("UL_MIN_FREE_DISK_MB", 200))

# ==========================================
# 2. EXCEPTIONS & ERROR CLASSIFICATION
# ==========================================

class FatalUploadError(Exception):
    """Unrecoverable — stops the upload immediately."""

class SessionLostError(Exception):
    """Server returned 404 for an active session — triggers session re-creation."""

class ChecksumMismatchError(Exception):
    """Server echoed a checksum that does not match what we sent."""

class DiskFullError(FatalUploadError):
    """Local disk is full — cannot read/write staging files."""     # FIX 3

class FileAccessError(FatalUploadError):
    """File locked by antivirus or another process."""              # FIX 3

class IntegrityError(FatalUploadError):
    """Server-side MD5 does not match client-side MD5 after upload."""  # FIX 1

class AllEndpointsFailedError(FatalUploadError):
    """Primary and all fallback endpoints are unreachable."""       # FIX 4

class ErrorCategory(Enum):
    NETWORK         = auto()
    SERVER_OVERLOAD = auto()
    CLIENT_ERROR    = auto()
    CHECKSUM        = auto()
    FATAL           = auto()

def classify_error(exc: Exception) -> ErrorCategory:
    if isinstance(exc, ChecksumMismatchError):
        return ErrorCategory.CHECKSUM
    if isinstance(exc, (httpx.ConnectError, httpx.ReadError, httpx.WriteError,
                        httpx.RemoteProtocolError, httpx.TimeoutException)):
        return ErrorCategory.NETWORK
    if isinstance(exc, asyncio.TimeoutError):
        return ErrorCategory.NETWORK
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status in (429, 502, 503, 504):
            return ErrorCategory.SERVER_OVERLOAD
        if 400 <= status < 500:
            return ErrorCategory.CLIENT_ERROR
    return ErrorCategory.FATAL

# ==========================================
# 3. STRUCTURED JSON LOGGING
# ==========================================

_LOG_SKIP: frozenset = frozenset({
    "args", "asctime", "created", "exc_info", "exc_text", "filename",
    "funcName", "levelname", "levelno", "lineno", "message", "module",
    "msecs", "msg", "name", "pathname", "process", "processName",
    "relativeCreated", "stack_info", "thread", "threadName", "taskName",
})

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()
        out: Dict[str, object] = {
            "ts":     self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S"),
            "level":  record.levelname,
            "logger": record.name,
            "msg":    record.message,
        }
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        for k, v in record.__dict__.items():
            if k not in _LOG_SKIP:
                out[k] = v
        return json.dumps(out, default=str)

def configure_json_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

from logger import get_enterprise_logger
_base_log = get_enterprise_logger("EnterpriseUploader")

class StructuredLogger:
    """Wraps the standard logger to move kwargs into the 'extra' dictionary."""
    def __init__(self, logger):
        self.logger = logger

    def debug(self, msg, **kwargs):
        self.logger.debug(msg, extra=kwargs)

    def info(self, msg, **kwargs):
        self.logger.info(msg, extra=kwargs)

    def warning(self, msg, **kwargs):
        self.logger.warning(msg, extra=kwargs)

    def error(self, msg, **kwargs):
        if 'exc_info' in kwargs:
            exc = kwargs.pop('exc_info')
            self.logger.error(msg, exc_info=exc, extra=kwargs)
        else:
            self.logger.error(msg, extra=kwargs)

    def critical(self, msg, **kwargs):
        self.logger.critical(msg, extra=kwargs)

log = StructuredLogger(_base_log)

# ==========================================
# 4. UPLOAD METRICS
# ==========================================

class UploadMetrics:
    def __init__(self, total_size: int, initial_offset: int = 0) -> None:
        self.total_size      = total_size
        self.bytes_uploaded  = initial_offset
        self.start_time      = time.monotonic()
        self.retry_count     = 0
        self.failed_chunks   = 0
        self._latencies: list = []
        self._max_lat         = 50

    def add_bytes(self, n: int) -> None:
        self.bytes_uploaded += n

    def reset_offset(self, offset: int) -> None:
        self.bytes_uploaded = offset

    def record_chunk_latency(self, elapsed: float) -> None:
        if len(self._latencies) >= self._max_lat:
            self._latencies.pop(0)
        self._latencies.append(elapsed)

    def record_retry(self) -> None:
        self.retry_count += 1

    def record_failed_chunk(self) -> None:
        self.failed_chunks += 1

    def get_progress(self) -> dict:
        pct     = (self.bytes_uploaded / self.total_size * 100) if self.total_size else 100.0
        elapsed = time.monotonic() - self.start_time
        speed   = self.bytes_uploaded / elapsed if elapsed > 0 else 0.0
        avg_lat = (sum(self._latencies) / len(self._latencies)) if self._latencies else 0.0
        return {
            "percent":         round(pct, 1),
            "speed_mbs":       round(speed / (1024 * 1024), 2),
            "retry_count":     self.retry_count,
            "failed_chunks":   self.failed_chunks,
            "avg_chunk_lat_s": round(avg_lat, 3),
        }

# ==========================================
# 5. RATE LIMITER (token bucket)
# ==========================================

class TokenBucketRateLimiter:
    def __init__(self, rate_bps: int, burst_multiplier: float = 2.0) -> None:
        self._rate     = rate_bps
        self._capacity = rate_bps * burst_multiplier
        self._tokens   = float(self._capacity)
        self._last_ts  = time.monotonic()
        self._enabled  = rate_bps > 0

    async def acquire(self, n_bytes: int) -> None:
        if not self._enabled:
            return
        now           = time.monotonic()
        elapsed       = now - self._last_ts
        self._tokens  = min(self._capacity, self._tokens + elapsed * self._rate)
        self._last_ts = now
        if self._tokens >= n_bytes:
            self._tokens -= n_bytes
            return
        deficit      = n_bytes - self._tokens
        wait         = deficit / self._rate
        self._tokens = 0.0
        await asyncio.sleep(wait)

# ==========================================
# 6. NETWORK MONITOR
# ==========================================

class NetworkMonitor:
    def __init__(self, cfg: UploadConfig) -> None:
        self._host     = cfg.net_check_host
        self._port     = cfg.net_check_port
        self._interval = cfg.net_check_interval
        self._timeout  = cfg.net_wait_timeout
        self.network_up = asyncio.Event()
        self.network_up.set()
        self._task: Optional[asyncio.Task] = None

    async def _probe(self) -> bool:
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port), timeout=3.0
            )
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return True
        except asyncio.TimeoutError:
            return False
        except OSError as e:
            # FIX 4: Explicitly catch DNS resolution failures and connection refused
            # errno 11001 = WSAHOST_NOT_FOUND (Windows), errno -2 = NXDOMAIN (Linux)
            log.debug(f"Network probe OS error (errno={e.errno}): {e}")
            return False
        except Exception:
            return False

    async def _loop(self) -> None:
        while True:
            up = await self._probe()
            if up and not self.network_up.is_set():
                log.info("Network restored", event="network_restored")
                self.network_up.set()
            elif not up and self.network_up.is_set():
                log.warning("Network down detected", event="network_down")
                self.network_up.clear()
            await asyncio.sleep(self._interval)

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="network-monitor")

    def stop(self) -> None:
        if self._task:
            self._task.cancel()

    async def wait_for_network(self, cancel_flag: asyncio.Event) -> None:
        if self.network_up.is_set():
            return
        log.info("Upload paused — waiting for network", event="network_wait")
        net_task    = asyncio.create_task(self.network_up.wait())
        cancel_task = asyncio.create_task(cancel_flag.wait())
        done, pending = await asyncio.wait(
            {net_task, cancel_task},
            return_when=asyncio.FIRST_COMPLETED,
            timeout=self._timeout,
        )
        for t in pending:
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        if cancel_flag.is_set():
            raise asyncio.CancelledError("Cancelled while waiting for network")
        if not self.network_up.is_set():
            raise FatalUploadError("Network unavailable for too long")

# ==========================================
# 7. ADAPTIVE CONTROLLER
# ==========================================

class AdaptiveController:
    def __init__(self, cfg: UploadConfig) -> None:
        self._cfg = cfg
        self.chunk_size:         int   = cfg.initial_chunk_size
        self._workers:           int   = cfg.initial_workers
        self.backpressure_delay: float = 0.0
        self._speed_ema:  float = 0.0
        self._ema_alpha:  float = 0.2
        self._ok_streak:  int   = 0
        self._fail_streak: int  = 0

    @property
    def max_workers(self) -> int:
        return self._workers

    def record_chunk_success(self, chunk_size: int, elapsed: float) -> None:
        speed = chunk_size / elapsed if elapsed > 0 else 0.0
        self._speed_ema = (
            speed if self._speed_ema == 0.0
            else self._ema_alpha * speed + (1.0 - self._ema_alpha) * self._speed_ema
        )
        self._ok_streak   += 1
        self._fail_streak  = 0
        cfg                = self._cfg

        if self._speed_ema >= cfg.speed_fast_threshold:
            self.chunk_size = min(cfg.max_chunk_size, int(self.chunk_size * 1.25))
        elif self._speed_ema <= cfg.speed_slow_threshold:
            self.chunk_size = max(cfg.min_chunk_size, int(self.chunk_size * 0.75))

        if self.backpressure_delay > 0.0 and self._ok_streak >= 5:
            self.backpressure_delay = max(0.0, self.backpressure_delay * 0.6)

        if self._ok_streak >= cfg.scale_up_after and self._workers < cfg.max_workers:
            self._workers    += 1
            self._ok_streak   = 0
            log.info("Scaled up workers", event="scale_up", workers=self._workers)

    def record_chunk_failure(self, category: ErrorCategory) -> None:
        self._fail_streak += 1
        self._ok_streak    = 0
        cfg                = self._cfg

        self.chunk_size = max(cfg.min_chunk_size, int(self.chunk_size * 0.80))

        if self._fail_streak >= 3 and self._workers > cfg.min_workers:
            self._workers -= 1
            log.warning("Scaled down workers", event="scale_down", workers=self._workers)

        if category == ErrorCategory.SERVER_OVERLOAD:
            self.backpressure_delay = min(
                30.0,
                self.backpressure_delay * 2.0 if self.backpressure_delay > 0.0 else 1.0,
            )

    def get_chunk_timeout(self) -> float:
        cfg = self._cfg
        if self._speed_ema > 0.0:
            expected = self.chunk_size / self._speed_ema
            return max(cfg.min_timeout, min(cfg.max_timeout, expected * 3.0))
        return cfg.initial_timeout

    def get_speed_mbs(self) -> float:
        return round(self._speed_ema / (1024 * 1024), 2)

# ==========================================
# 8. REAL COLLABORATORS
# ==========================================

class UploadLedger:
    """
    Persistence layer for resumable upload state.
    FIX 2: WAL + SYNCHRONOUS FULL — survives sudden power loss.
    """

    def __init__(self, db_path: str = "v4_upload_ledger.db") -> None:
        self.db_path = db_path
        self.lock    = threading.Lock()
        with self._conn() as c:
            # ── FIX 2: crash-safe SQLite ──────────────────────────────────
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=FULL")
            c.execute("PRAGMA wal_autocheckpoint=100")
            # ──────────────────────────────────────────────────────────────
            c.execute("""
                CREATE TABLE IF NOT EXISTS tus_uploads (
                    file_path  TEXT PRIMARY KEY,
                    study_uid  TEXT,
                    upload_url TEXT,
                    total_size INTEGER,
                    chunk_size INTEGER,
                    offset     INTEGER,
                    chunk_map  TEXT,
                    status     TEXT
                )
            """)

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def _checkpoint(self) -> None:
        """Force WAL checkpoint — call after critical updates."""  # FIX 2
        try:
            with self._conn() as c:
                c.execute("PRAGMA wal_checkpoint(FULL)")
        except Exception:
            pass

    def upsert_upload(self, file_path: str, study_uid: str, total_size: int, chunk_size: int) -> None:
        with self.lock, self._conn() as c:
            c.execute(
                "INSERT OR IGNORE INTO tus_uploads VALUES (?,?,''  ,?,?,0,'[]','pending')",
                (file_path, study_uid, total_size, chunk_size),
            )

    def get_upload_state(self, file_path: str) -> dict:
        with self.lock, self._conn() as c:
            c.row_factory = sqlite3.Row
            row = c.execute("SELECT * FROM tus_uploads WHERE file_path=?", (file_path,)).fetchone()
            return dict(row) if row else {}

    def update_session(self, file_path: str, upload_url: str, status: str) -> None:
        with self.lock, self._conn() as c:
            c.execute(
                "UPDATE tus_uploads SET upload_url=?, status=? WHERE file_path=?",
                (upload_url, status, file_path),
            )
        self._checkpoint()   # FIX 2: flush immediately after session change

    def flush_chunk_batch(self, file_path: str, chunk_indices: Set[int], byte_count: int) -> None:
        if not chunk_indices:
            return
        with self.lock, self._conn() as c:
            row = c.execute(
                "SELECT chunk_map, offset FROM tus_uploads WHERE file_path=?", (file_path,)
            ).fetchone()
            if row:
                cmap = set(json.loads(row[0]))
                cmap.update(chunk_indices)
                c.execute(
                    "UPDATE tus_uploads SET chunk_map=?, offset=? WHERE file_path=?",
                    (json.dumps(list(cmap)), row[1] + byte_count, file_path),
                )

    def mark_chunk_complete(self, file_path: str, chunk_idx: int, byte_count: int) -> None:
        self.flush_chunk_batch(file_path, {chunk_idx}, byte_count)
        self._checkpoint()   # FIX 2: immediate flush for sequential writes

    def mark_completed(self, file_path: str) -> None:
        with self.lock, self._conn() as c:
            c.execute("UPDATE tus_uploads SET status='completed' WHERE file_path=?", (file_path,))
        self._checkpoint()   # FIX 2


class TusClient:
    """
    HTTP/2 Tus protocol client with Zombie Socket Purging capabilities.
    """
    def __init__(self, api_url: str, timeout: float, proxy_url: str = "") -> None:
        self.api_url = api_url if api_url.endswith("/") else api_url + "/"
        self._timeout = timeout
        self._proxy_url = proxy_url
        self._build_client()

    def _build_client(self) -> None:
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=50)
        _proxy = (
            self._proxy_url
            or os.environ.get("HTTPS_PROXY")
            or os.environ.get("HTTP_PROXY")
            or os.environ.get("https_proxy")
            or os.environ.get("http_proxy")
            or ""
        )
        mounts = {}
        if _proxy:
            mounts = {
                "https://": httpx.AsyncHTTPTransport(proxy=_proxy),
                "http://":  httpx.AsyncHTTPTransport(proxy=_proxy),
            }
        custom_timeout = httpx.Timeout(connect=60.0, read=300.0, write=300.0, pool=15.0)
        self._client = httpx.AsyncClient(
            http2    = False,
            timeout=custom_timeout,
            verify   = False,
            limits   = limits,
            mounts   = mounts if mounts else None,
            follow_redirects = True
        )

    async def reset_client(self) -> None:
        """Destroys the zombie HTTP/2 connection pool and builds a fresh one."""
        old_client = self._client
        self._build_client()
        # Cleanly close the old socket pool in the background so we don't freeze
        try:
            asyncio.create_task(old_client.aclose())
        except Exception:
            pass

    async def close(self) -> None:
        await self._client.aclose()

    async def create_session(self, total_size: int, filename: str, study_uid: str, mode: str) -> str:
        name_b64 = base64.b64encode(filename.encode()).decode()
        uid_b64  = base64.b64encode(study_uid.encode()).decode()

        headers = {
            "Tus-Resumable":  "1.0.0",
            "Upload-Length":  str(total_size),
            "ngrok-skip-browser-warning": "true",
            "Upload-Metadata": f"filename {name_b64},study_uid {uid_b64}",
        }
        _api_key = os.environ.get("TUS_API_KEY", "")
        _api_user = os.environ.get("TUS_API_USER", "")
        
        if _api_key:
            headers["X-Api-Key"] = _api_key
        if _api_user:
            headers["X-Api-User"] = _api_user

        resp = await self._client.post(self.api_url, headers=headers)
    
        if resp.status_code == 409:
            return ""    # Server says use existing session

        loc = resp.headers.get("Location")
        if loc:
            upload_url = loc if loc.startswith("http") else urljoin(self.api_url, loc)
            
            # --- ADD THIS SAFEGUARD ---
            # If our base API is HTTPS, but the server returned HTTP, force it back to HTTPS
            if self.api_url.startswith("https://") and upload_url.startswith("http://"):
                upload_url = upload_url.replace("http://", "https://", 1)
                
            return upload_url
            
        raise FatalUploadError("Server didn't return a Location header")

    async def verify_session(self, upload_url: str) -> int:
        if not upload_url:
            return 0
        headers = {"Tus-Resumable": "1.0.0",
                   "ngrok-skip-browser-warning": "true"}

        _api_key = os.environ.get("TUS_API_KEY", "")
        _api_user = os.environ.get("TUS_API_USER", "")
        
        if _api_key:
            headers["X-Api-Key"] = _api_key
        if _api_user:
            headers["X-Api-User"] = _api_user

        resp = await self._client.head(upload_url, headers=headers)
        
        
        if resp.status_code == 404:
            raise SessionLostError("Server returned 404 for session")
        resp.raise_for_status()
        return int(resp.headers.get("Upload-Offset", 0))

    async def upload_chunk(self, upload_url: str, data: bytes, headers: dict, timeout: float = None) -> dict:
        _api_key = os.environ.get("TUS_API_KEY", "")
        _api_user = os.environ.get("TUS_API_USER", "")
        
        if _api_key:
            headers["X-Api-Key"] = _api_key
        if _api_user:
            headers["X-Api-User"] = _api_user
            
        kwargs = {}
        if timeout is not None:
            kwargs["timeout"] = timeout
            
        # Passing timeout natively forces httpx to drop dead streams!
        resp = await self._client.patch(upload_url, content=data, headers=headers, **kwargs)
        resp.raise_for_status()
        return dict(resp.headers)

    # ── FIX 1: REAL FINALIZATION ──────────────────────────────────────────
    async def finalize_upload(self, upload_url: str, full_md5: str) -> None:
        """
        Two-step integrity verification:
          1. HEAD check  — server offset must equal declared total size
          2. /verify GET — server MD5 must match client-computed MD5

        Raises FatalUploadError / IntegrityError on any mismatch.
        """
        if not upload_url:
            return

        # ── Step 1: HEAD check ────────────────────────────────────────────
        try:
            headers = {"Tus-Resumable": "1.0.0"}
            _api_key = os.environ.get("TUS_API_KEY", "")
            if _api_key:
              headers["X-Api-Key"] = _api_key

            head = await self._client.head(upload_url, headers=headers)
            head.raise_for_status()
            server_offset = int(head.headers.get("Upload-Offset", 0))
            total_size    = int(head.headers.get("Upload-Length", 0))
            if server_offset < total_size:
                raise FatalUploadError(
                    f"Upload incomplete after all chunks: "
                    f"server_offset={server_offset} < total={total_size}"
                )
        except (FatalUploadError, IntegrityError):
            raise
        except httpx.HTTPStatusError as e:
            raise FatalUploadError(f"Final HEAD check HTTP error {e.response.status_code}") from e
        except httpx.RequestError as e:
            raise FatalUploadError(f"Final HEAD check network error: {e}") from e

        # ── Step 2: /verify endpoint ──────────────────────────────────────
        if not full_md5:
            return   # No hash available (shouldn't happen, but safe fallback)

        verify_url = upload_url.rstrip("/") + "/verify"
        try:
            headers = {"Tus-Resumable": "1.0.0"}
            if _api_key:
              headers["X-Api-Key"] = _api_key

            resp = await self._client.get(verify_url, headers=headers, timeout=120.0)    # Computing MD5 server-side takes time on large files
           
            if resp.status_code == 404:
                log.warning(
                    "Server has no /verify endpoint — skipping MD5 check",
                    event="verify_skipped",
                )
                return            # Server doesn't support verify — HEAD was enough

            resp.raise_for_status()
            data = resp.json()

            if not data.get("complete"):
                raise FatalUploadError(
                    f"Server reports upload not complete: status={data.get('status')}"
                )
            if not data.get("size_match"):
                raise FatalUploadError(
                    f"File size mismatch on server: "
                    f"actual={data.get('actual_size')} declared={data.get('total_size')}"
                )

            server_md5 = data.get("md5", "")
            if server_md5 and server_md5 != full_md5:
                raise IntegrityError(
                    f"MD5 mismatch — data corruption detected! "
                    f"server={server_md5} client={full_md5} "
                    f"file={data.get('filename')}"
                )

            log.info(
                "Integrity verified",
                event="integrity_ok",
                md5=full_md5,
                filename=data.get("filename"),
            )

        except (FatalUploadError, IntegrityError):
            raise
        except Exception as e:
            # Verify endpoint is optional; log and continue — HEAD already passed
            log.warning(
                f"Verify endpoint error (non-fatal, HEAD already passed): {e}",
                event="verify_error",
            )
    # ──────────────────────────────────────────────────────────────────────

# ==========================================
# 9. UPLOAD CONTEXT
# ==========================================

@dataclass
class UploadContext:
    tus:             TusClient
    ledger:          UploadLedger
    controller:      AdaptiveController
    network_monitor: NetworkMonitor
    rate_limiter:    TokenBucketRateLimiter
    file_path:       str
    upload_url:      str
    total_size:      int
    cancel_flag:     asyncio.Event
    correlation_id:  str
    cfg:             UploadConfig
    chunk_size:      int

# ==========================================
# 10. ABSTRACT STRATEGY BASE
# ==========================================

class UploadStrategy(ABC):
    def __init__(self, ctx: UploadContext) -> None:
        self.ctx  = ctx
        # Create a standard adapter with the base logger, then wrap it in our new StructuredLogger
        adapter = logging.LoggerAdapter(_base_log, {"correlation_id": ctx.correlation_id})
        
        self._log = StructuredLogger(adapter)

    async def _sleep_interruptible(self, delay: float) -> None:
        if self.ctx.cancel_flag.is_set():
            raise asyncio.CancelledError()
        try:
            await asyncio.wait_for(self.ctx.cancel_flag.wait(), timeout=delay)
            raise asyncio.CancelledError("Cancelled during backoff")
        except asyncio.TimeoutError:
            pass

    async def _backoff(
        self, attempt: int, error: Exception, context: str, category: ErrorCategory
    ) -> None:
        cfg = self.ctx.cfg
        if attempt >= cfg.max_retries - 1:
            return
            
        if category == ErrorCategory.NETWORK:
            # ── THE KILL SWITCH ──
            # Force the client to dump dead sockets before we wait for the network
            self._log.warning("Network drop detected. Purging dead sockets.", event="socket_purge")
            await self.ctx.tus.reset_client()
            await self.ctx.network_monitor.wait_for_network(self.ctx.cancel_flag)
            
        delay = min(30.0, 2.0 * (2 ** attempt)) + random.uniform(0, 1)
        self._log.debug(
            f"Retry {attempt + 1}/{cfg.max_retries} — {context}",
            event="retry", attempt=attempt, delay=round(delay, 1),
            error_type=type(error).__name__, category=category.name,
        )
        await self._sleep_interruptible(delay)

    @abstractmethod
    async def execute(self, state: dict, metrics: UploadMetrics) -> Optional[str]:
        pass

# ==========================================
# 11. PARALLEL UPLOADER
# ==========================================

class ParallelUploader(UploadStrategy):
    async def execute(self, state: dict, metrics: UploadMetrics) -> Optional[str]:
        ctx        = self.ctx
        server_off = await ctx.tus.verify_session(ctx.upload_url)
        chunk_size = ctx.chunk_size

        raw_map: Set[int] = (
            set(json.loads(state["chunk_map"])) if state.get("chunk_map") else set()
        )
        chunk_map: Set[int] = {
            idx for idx in raw_map
            if min((idx + 1) * chunk_size, ctx.total_size) <= server_off
        }

        total_chunks = (ctx.total_size + chunk_size - 1) // chunk_size
        pending      = [i for i in range(total_chunks) if i not in chunk_map]

        if not pending:
            return None

        queue        = asyncio.Queue(maxsize=ctx.controller.max_workers * 2)
        semaphore    = asyncio.Semaphore(ctx.controller.max_workers)
        buffer_state = {"chunks": set(), "bytes": 0}
        buffer_lock  = asyncio.Lock()
        upload_done  = asyncio.Event()

        async def final_flush() -> None:
            async with buffer_lock:
                if buffer_state["chunks"]:
                    ctx.ledger.flush_chunk_batch(
                        ctx.file_path, buffer_state["chunks"], buffer_state["bytes"]
                    )
                    buffer_state["chunks"].clear()
                    buffer_state["bytes"] = 0

        async def db_flusher() -> None:
            try:
                while not upload_done.is_set() and not ctx.cancel_flag.is_set():
                    await asyncio.sleep(2.0)
                    await final_flush()
            except asyncio.CancelledError:
                pass

        async def producer() -> None:
            try:
                for idx in pending:
                    if ctx.cancel_flag.is_set():
                        break
                    await queue.put(idx)
                for _ in range(ctx.controller.max_workers):
                    await queue.put(None)
            except asyncio.CancelledError:
                pass

        async def worker(worker_id: int) -> None:
            try:
                # ── FIX 3: handle file-locked / permission errors ──────────
                try:
                    fh_ctx = aiofiles.open(ctx.file_path, "rb")
                except PermissionError as exc:
                    raise FileAccessError(
                        f"Cannot open {ctx.file_path} — locked by antivirus or another process: {exc}"
                    ) from exc
                # ──────────────────────────────────────────────────────────
                async with fh_ctx as fh:
                    while True:
                        chunk_idx = await queue.get()
                        try:
                            if chunk_idx is None:
                                break
                            if ctx.cancel_flag.is_set():
                                continue
                            await self._process_chunk(
                                chunk_idx, chunk_size, total_chunks,
                                semaphore, buffer_state, buffer_lock, metrics, fh,
                            )
                        except Exception as exc:
                            ctx.cancel_flag.set()
                            raise exc
                        finally:
                            queue.task_done()
            except asyncio.CancelledError:
                pass

        async def queue_waiter() -> None:
            await queue.join()
            upload_done.set()

        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(db_flusher(),   name="db-flusher")
                tg.create_task(producer(),     name="producer")
                tg.create_task(queue_waiter(), name="queue-waiter")
                for i in range(ctx.controller.max_workers):
                    tg.create_task(worker(i), name=f"worker-{i}")
        except ExceptionGroup as eg:
            upload_done.set()
            for exc_type in (
                FatalUploadError, FileAccessError, DiskFullError,
                SessionLostError, ChecksumMismatchError, asyncio.CancelledError,
            ):
                for exc in eg.exceptions:
                    if isinstance(exc, exc_type):
                        raise exc
            raise eg.exceptions[0]
        finally:
            await final_flush()

        return None

    async def _process_chunk(
        self,
        chunk_idx:    int,
        chunk_size:   int,
        total_chunks: int,
        semaphore:    asyncio.Semaphore,
        buffer_state: dict,
        buffer_lock:  asyncio.Lock,
        metrics:      UploadMetrics,
        fh,
    ) -> None:
        ctx        = self.ctx
        start_byte = chunk_idx * chunk_size
        end_byte   = min(start_byte + chunk_size, ctx.total_size)

        # ── FIX 3: wrap file read in OS-error guards ───────────────────────
        try:
            await fh.seek(start_byte)
            chunk_data = await fh.read(end_byte - start_byte)
        except PermissionError as exc:
            raise FileAccessError(
                f"Chunk {chunk_idx}: file locked — antivirus may be scanning"
            ) from exc
        except OSError as exc:
            if exc.errno in (_errno.ENOSPC, _errno.EDQUOT if hasattr(_errno, "EDQUOT") else -1):
                raise DiskFullError(f"Disk full while reading chunk {chunk_idx}") from exc
            raise FatalUploadError(f"Chunk {chunk_idx} read OS error: {exc}") from exc
        # ──────────────────────────────────────────────────────────────────

        b64_md5 = base64.b64encode(
            hashlib.md5(chunk_data, usedforsecurity=False).digest()
        ).decode()
        headers = {
            "Tus-Resumable":   "1.0.0",
            "Upload-Offset":   str(start_byte),
            "Upload-Checksum": f"md5 {b64_md5}",
            "X-Chunk-Index":   str(chunk_idx),
            "X-Chunk-Start":   str(start_byte),
            "X-Chunk-End":     str(end_byte),
            "Content-Type":    "application/offset+octet-stream",
        }

        cfg = ctx.cfg
        for attempt in range(cfg.max_retries):
            if ctx.cancel_flag.is_set():
                raise asyncio.CancelledError()

            await ctx.rate_limiter.acquire(len(chunk_data))

            if ctx.controller.backpressure_delay > 0.0:
                jitter = ctx.controller.backpressure_delay * random.uniform(0.5, 1.5)
                await self._sleep_interruptible(jitter)

            try:
                async with semaphore:
                    t0            = time.monotonic()
                    chunk_timeout = ctx.controller.get_chunk_timeout()
                    
                    # ── NATIVE TIMEOUT HANDLING ──
                    resp_headers = await ctx.tus.upload_chunk(
                        ctx.upload_url, chunk_data, headers, timeout=chunk_timeout
                    )
                    elapsed = time.monotonic() - t0

                server_cksum = (resp_headers or {}).get("Upload-Checksum", "")
                if server_cksum and server_cksum != f"md5 {b64_md5}":
                    raise ChecksumMismatchError(
                        f"Chunk {chunk_idx}: sent md5 {b64_md5}, server echoed {server_cksum}"
                    )

                ctx.controller.record_chunk_success(len(chunk_data), elapsed)
                metrics.add_bytes(len(chunk_data))
                metrics.record_chunk_latency(elapsed)

                async with buffer_lock:
                    buffer_state["chunks"].add(chunk_idx)
                    buffer_state["bytes"] += len(chunk_data)

                if chunk_idx % 10 == 0 or chunk_idx == total_chunks - 1:
                    prog = metrics.get_progress()
                    self._log.info(
                        f"[Parallel] chunk {chunk_idx}/{total_chunks}",
                        event="chunk_ok", **prog,
                        workers=ctx.controller.max_workers,
                        chunk_kb=ctx.controller.chunk_size // 1024,
                        speed_controller_mbs=ctx.controller.get_speed_mbs(),
                    )
                return

            except asyncio.TimeoutError as exc:
                cat = ErrorCategory.NETWORK
                ctx.controller.record_chunk_failure(cat)
                metrics.record_retry()
                await self._backoff(attempt, exc, "Chunk timeout", cat)

            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                cat    = classify_error(exc)
                if status in (429, 502, 503, 504):
                    ctx.controller.record_chunk_failure(cat)
                    metrics.record_retry()
                    await self._backoff(attempt, exc, f"Server overload HTTP {status}", cat)
                elif status == 409:
                    metrics.add_bytes(len(chunk_data))
                    async with buffer_lock:
                        buffer_state["chunks"].add(chunk_idx)
                        buffer_state["bytes"] += len(chunk_data)
                    return
                elif 400 <= status < 500:
                    raise FatalUploadError(f"Client error HTTP {status}: {exc.response.text}")

            except ChecksumMismatchError as exc:
                metrics.record_failed_chunk()
                metrics.record_retry()
                self._log.warning(str(exc), event="checksum_mismatch", chunk_idx=chunk_idx, attempt=attempt)
                await self._backoff(attempt, exc, "Checksum mismatch", ErrorCategory.CHECKSUM)

            except (httpx.RequestError, httpx.TimeoutException) as exc:
                cat = classify_error(exc)
                ctx.controller.record_chunk_failure(cat)
                metrics.record_retry()
                await self._backoff(attempt, exc, "Network error", cat)

            except (FileAccessError, DiskFullError, FatalUploadError):
                raise    # Do not retry unrecoverable local errors

            except Exception as exc:
                ctx.controller.record_chunk_failure(ErrorCategory.FATAL)
                await self._backoff(attempt, exc, "Unknown error", ErrorCategory.FATAL)

        raise FatalUploadError(f"Max retries exhausted for chunk {chunk_idx}")


# ==========================================
# 12. SEQUENTIAL UPLOADER
# ==========================================

class SequentialUploader(UploadStrategy):
    async def execute(self, state: dict, metrics: UploadMetrics) -> Optional[str]:
        ctx          = self.ctx
        offset       = await ctx.tus.verify_session(ctx.upload_url)
        chunk_size   = ctx.chunk_size
        total_chunks = (ctx.total_size + chunk_size - 1) // chunk_size
        hasher       = hashlib.md5(usedforsecurity=False)

        # ── FIX 3: guard file open ─────────────────────────────────────────
        try:
            fh_ctx = aiofiles.open(ctx.file_path, "rb")
        except PermissionError as exc:
            raise FileAccessError(
                f"Cannot open {ctx.file_path} for sequential upload — "
                f"locked by antivirus or another process: {exc}"
            ) from exc
        # ──────────────────────────────────────────────────────────────────

        async with fh_ctx as f:
            remaining = offset
            while remaining > 0:
                block = await f.read(min(8 * 1024 * 1024, remaining))
                if not block:
                    break
                hasher.update(block)
                remaining -= len(block)

            await f.seek(offset)

            while offset < ctx.total_size:
                if ctx.cancel_flag.is_set():
                    raise asyncio.CancelledError()

                # ── FIX 3: guard chunk read ────────────────────────────────
                try:
                    chunk_data = await f.read(chunk_size)
                except PermissionError as exc:
                    raise FileAccessError(f"Read blocked at offset {offset}: {exc}") from exc
                except OSError as exc:
                    if exc.errno in (_errno.ENOSPC, _errno.EDQUOT if hasattr(_errno, "EDQUOT") else -1):
                        raise DiskFullError(f"Disk full at offset {offset}") from exc
                    raise FatalUploadError(f"Read OS error at offset {offset}: {exc}") from exc
                # ──────────────────────────────────────────────────────────

                if not chunk_data:
                    break

                current_idx = offset // chunk_size
                b64_md5     = base64.b64encode(
                    hashlib.md5(chunk_data, usedforsecurity=False).digest()
                ).decode()
                headers = {
                    "Tus-Resumable":   "1.0.0",
                    "Upload-Offset":   str(offset),
                    "Upload-Checksum": f"md5 {b64_md5}",
                    "Content-Type":    "application/offset+octet-stream",
                }

                for attempt in range(ctx.cfg.max_retries):
                    if ctx.cancel_flag.is_set():
                        raise asyncio.CancelledError()

                    await ctx.rate_limiter.acquire(len(chunk_data))

                    if ctx.controller.backpressure_delay > 0.0:
                        await self._sleep_interruptible(ctx.controller.backpressure_delay)

                    try:
                        t0            = time.monotonic()
                        chunk_timeout = ctx.controller.get_chunk_timeout()
                        
                        # ── NATIVE TIMEOUT HANDLING ──
                        resp_headers = await ctx.tus.upload_chunk(
                            ctx.upload_url, chunk_data, headers, timeout=chunk_timeout
                        )
                        elapsed = time.monotonic() - t0

                        server_cksum = (resp_headers or {}).get("Upload-Checksum", "")
                        if server_cksum and server_cksum != f"md5 {b64_md5}":
                            raise ChecksumMismatchError(f"Chunk {current_idx} checksum mismatch")

                        hasher.update(chunk_data)
                        ctx.controller.record_chunk_success(len(chunk_data), elapsed)
                        ctx.ledger.mark_chunk_complete(ctx.file_path, current_idx, len(chunk_data))
                        offset += len(chunk_data)
                        metrics.add_bytes(len(chunk_data))
                        metrics.record_chunk_latency(elapsed)

                        if current_idx % 10 == 0 or offset >= ctx.total_size:
                            prog = metrics.get_progress()
                            self._log.info(
                                f"[Sequential] chunk {current_idx}/{total_chunks}",
                                event="chunk_ok", **prog,
                            )
                        break

                    except asyncio.TimeoutError as exc:
                        cat = ErrorCategory.NETWORK
                        ctx.controller.record_chunk_failure(cat)
                        metrics.record_retry()
                        await self._backoff(attempt, exc, "Chunk timeout", cat)

                    except httpx.HTTPStatusError as exc:
                        status = exc.response.status_code
                        cat    = classify_error(exc)
                        if status in (429, 502, 503, 504):
                            ctx.controller.record_chunk_failure(cat)
                            metrics.record_retry()
                            await self._backoff(attempt, exc, f"Server overload HTTP {status}", cat)
                        elif status == 409:
                            self._log.warning("Offset mismatch (409) — reconciling", event="offset_reconcile")
                            new_offset = await ctx.tus.verify_session(ctx.upload_url)
                            if new_offset > offset:
                                confirmed_end = min(len(chunk_data), new_offset - offset)
                                hasher.update(chunk_data[:confirmed_end])
                                if new_offset > offset + len(chunk_data):
                                    gap_start = offset + len(chunk_data)
                                    await f.seek(gap_start)
                                    gap = await f.read(new_offset - gap_start)
                                    if gap:
                                        hasher.update(gap)
                            offset = new_offset
                            await f.seek(new_offset)
                            break
                        elif 400 <= status < 500:
                            raise FatalUploadError(f"Client error HTTP {status}: {exc.response.text}")

                    except ChecksumMismatchError as exc:
                        metrics.record_failed_chunk()
                        metrics.record_retry()
                        self._log.warning(str(exc), event="checksum_mismatch", attempt=attempt)
                        await self._backoff(attempt, exc, "Checksum mismatch", ErrorCategory.CHECKSUM)

                    except (httpx.RequestError, httpx.TimeoutException, OSError, ConnectionError) as exc:
                        # ── INTERFACE SWITCH FIX ──
                        # Force all violent OS-level socket drops to be treated as recoverable network events
                        # This guarantees 'reset_client()' will run and rebuild the pool on the new Wi-Fi adapter
                        cat = ErrorCategory.NETWORK  
                        ctx.controller.record_chunk_failure(cat)
                        metrics.record_retry()
                        
                        self._log.warning(
                            f"Hard adapter disconnect detected: {type(exc).__name__}", 
                            event="adapter_switch"
                        )
                        await self._backoff(attempt, exc, "Hard Network Switch", cat)

                    except (FileAccessError, DiskFullError, FatalUploadError):
                        raise

                    except Exception as exc:
                        ctx.controller.record_chunk_failure(ErrorCategory.FATAL)
                        await self._backoff(attempt, exc, "Unknown error", ErrorCategory.FATAL)
                else:
                    raise FatalUploadError(f"Sequential upload permanently failed at offset {offset}")

        return hasher.hexdigest()

# ==========================================
# 13. ENTERPRISE UPLOAD ENGINE
# ==========================================

class EnterpriseUploadEngine:
    _VALID_MODES: frozenset = frozenset({"parallel", "sequential"})

    def __init__(
        self,
        ledger:            UploadLedger,
        cfg:               UploadConfig,
        api_url:           str,
        mode_override:     Optional[str] = None,
        fallback_api_urls: Optional[List[str]] = None,
    ) -> None:
        self.ledger       = ledger
        self.cfg          = cfg
        self.controller   = AdaptiveController(cfg)
        self.net_monitor  = NetworkMonitor(cfg)
        self.rate_limiter = TokenBucketRateLimiter(cfg.rate_limit_bps)
        self.cancel_flag  = asyncio.Event()

        # ── FIX 4: fallback URL list ───────────────────────────────────────
        env_fallbacks = [u.strip() for u in cfg.fallback_urls.split(",") if u.strip()]
        self._all_urls: List[str] = [api_url] + (fallback_api_urls or []) + env_fallbacks
        self._current_url_idx: int = 0
        # ──────────────────────────────────────────────────────────────────

        raw_mode = mode_override if mode_override is not None else cfg.default_mode
        if raw_mode not in self._VALID_MODES:
            raise ValueError(f"Invalid upload mode {raw_mode!r}. Must be one of {sorted(self._VALID_MODES)}.")
        self._mode: str = raw_mode

        self._upload_in_progress: bool = False
        self._signals_registered: bool = False
        self._monitor_started:    bool = False

        # Create TusClient with current (primary) URL
        self.tus = TusClient(self._all_urls[0], cfg.initial_timeout, cfg.proxy_url)

    # ── FIX 4: try next endpoint on connection failure ──────────────────
    def _rotate_endpoint(self) -> bool:
        """Try the next endpoint in the fallback list. Returns False if all exhausted."""
        self._current_url_idx += 1
        if self._current_url_idx >= len(self._all_urls):
            return False
        new_url = self._all_urls[self._current_url_idx]
        log.warning(
            f"Rotating to fallback endpoint #{self._current_url_idx}: {new_url}",
            event="endpoint_fallback",
        )
        self.tus.api_url = new_url if new_url.endswith("/") else new_url + "/"
        return True
    # ────────────────────────────────────────────────────────────────────

    @property
    def current_mode(self) -> str:
        return self._mode

    def set_mode(self, mode: str) -> None:
        if mode not in self._VALID_MODES:
            raise ValueError(f"Invalid upload mode {mode!r}.")
        if self._upload_in_progress:
            raise RuntimeError("Cannot change mode while upload is in progress.")
        old, self._mode = self._mode, mode
        log.info(f"Mode changed: {old!r} → {mode!r}", event="mode_change")

    def _setup_signals(self, loop: asyncio.AbstractEventLoop) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._on_signal, sig)
            except (NotImplementedError, AttributeError):
                try:
                    # Windows fallback
                    signal.signal(sig, lambda s, f: self.cancel())
                except ValueError:
                    # FIX: We are running in a background thread (V4NetworkLoop).
                    # Python blocks signal registration here. Safely ignore it,
                    # as the main app thread is handling the master OS shutdown.
                    pass
    def _on_signal(self, sig: signal.Signals) -> None:
        log.warning(f"Received {sig.name} — graceful shutdown", event="shutdown_signal", signal=sig.name)
        self.cancel()

    def cancel(self) -> None:
        log.warning("Cancellation requested", event="cancel")
        self.cancel_flag.set()

    async def close(self) -> None:
        self.net_monitor.stop()
        await self.tus.close()

    # ── FIX 3: pre-flight disk space check ───────────────────────────────
    @staticmethod
    def _check_disk_space(file_path: str, min_free_mb: int) -> None:
        try:
            drive = os.path.splitdrive(os.path.abspath(file_path))[0] or "/"
            stat  = shutil.disk_usage(drive)
            free_mb = stat.free // (1024 * 1024)
            if free_mb < min_free_mb:
                raise DiskFullError(
                    f"Insufficient disk space: {free_mb}MB free, need at least {min_free_mb}MB"
                )
        except DiskFullError:
            raise
        except Exception:
            pass   # If we can't check, proceed optimistically
    # ────────────────────────────────────────────────────────────────────

    @staticmethod
    async def _stream_hash_file(file_path: str, cancel_flag: asyncio.Event) -> str:
        hasher = hashlib.md5(usedforsecurity=False)
        try:
            async with aiofiles.open(file_path, "rb") as f:
                while chunk := await f.read(8 * 1024 * 1024):
                    if cancel_flag.is_set():
                        raise asyncio.CancelledError("Hash cancelled")
                    hasher.update(chunk)
        except PermissionError as exc:
            raise FileAccessError(f"Hash: cannot read {file_path}: {exc}") from exc
        return hasher.hexdigest()

    async def process_upload(self, file_path: str, study_uid: str) -> bool:
        loop = asyncio.get_running_loop()
        if not self._signals_registered:
            self._setup_signals(loop)
            self._signals_registered = True
        if not self._monitor_started:
            self.net_monitor.start()
            self._monitor_started = True

        # ── FIX 3: disk check before we start ─────────────────────────────
        self._check_disk_space(file_path, self.cfg.min_free_disk_mb)
        # ──────────────────────────────────────────────────────────────────

        # ── FIX 3: verify file exists and is readable ──────────────────────
        if not os.path.isfile(file_path):
            log.error(f"File not found: {file_path}", event="file_missing")
            return False
        try:
            with open(file_path, "rb") as _probe:
                _probe.read(1)
        except PermissionError as exc:
            log.error(f"File locked at start: {file_path}", event="file_locked", exc_info=True)
            return False
        # ──────────────────────────────────────────────────────────────────

        selected_mode: str = self._mode
        self._upload_in_progress = True
        correlation_id = str(uuid.uuid4())
        upload_url: Optional[str] = None
        total_size = os.path.getsize(file_path)

        self.ledger.upsert_upload(file_path, study_uid, total_size, self.controller.chunk_size)

        session_chunk_size = self.controller.chunk_size
        init_state = self.ledger.get_upload_state(file_path)
        metrics    = UploadMetrics(total_size, init_state.get("offset", 0))

        log.info(
            "Upload starting",
            event="upload_start",
            file=os.path.basename(file_path),
            size_mb=round(total_size / (1024 * 1024), 1),
            mode=selected_mode,
            correlation_id=correlation_id,
        )

        hash_task: Optional[asyncio.Task] = None

        try:
            for session_attempt in range(3):
                if self.cancel_flag.is_set():
                    raise asyncio.CancelledError()

                state      = self.ledger.get_upload_state(file_path)
                upload_url = state.get("upload_url")
                metrics.reset_offset(state.get("offset", 0))

                # ── FIX 4: retry session creation across endpoints ─────────
                session_created = False
                for _ in range(len(self._all_urls)):
                    try:
                        if not upload_url:
                            upload_url = await self.tus.create_session(
                                total_size, os.path.basename(file_path), study_uid, selected_mode
                            )
                            self.ledger.update_session(file_path, upload_url, "in_progress")
                        session_created = True
                        break
                    except (httpx.ConnectError, httpx.RemoteProtocolError, OSError) as exc:
                        log.warning(
                            f"Endpoint {self.tus.api_url} unreachable: {exc}",
                            event="endpoint_error",
                        )
                        if not self._rotate_endpoint():
                            raise AllEndpointsFailedError("All upload endpoints are unreachable") from exc
                        upload_url = None   # Force re-creation on next endpoint
                    except Exception as exc:
                        raise FatalUploadError(f"Session init failed: {exc}") from exc

                if not session_created:
                    raise AllEndpointsFailedError("Could not create upload session on any endpoint")
                # ──────────────────────────────────────────────────────────

                ctx = UploadContext(
                    tus             = self.tus,
                    ledger          = self.ledger,
                    controller      = self.controller,
                    network_monitor = self.net_monitor,
                    rate_limiter    = self.rate_limiter,
                    file_path       = file_path,
                    upload_url      = upload_url,
                    total_size      = total_size,
                    cancel_flag     = self.cancel_flag,
                    correlation_id  = correlation_id,
                    cfg             = self.cfg,
                    chunk_size      = session_chunk_size,
                )

                try:
                    if selected_mode == "parallel":
                        strategy  = ParallelUploader(ctx)
                        hash_task = asyncio.create_task(
                            self._stream_hash_file(file_path, self.cancel_flag),
                            name="background-hasher",
                        )
                        await strategy.execute(state, metrics)
                        full_hash = await hash_task
                        hash_task = None
                    elif selected_mode == "sequential":
                        strategy  = SequentialUploader(ctx)
                        full_hash = await strategy.execute(state, metrics)
                    else:
                        raise FatalUploadError(f"Unexpected mode {selected_mode!r}")

                except SessionLostError:
                    log.warning(
                        "Session lost (404) — re-creating",
                        event="session_lost", correlation_id=correlation_id, attempt=session_attempt,
                    )
                    self.ledger.update_session(file_path, "", "pending")
                    upload_url = None
                    if hash_task and not hash_task.done():
                        hash_task.cancel()
                        try:
                            await hash_task
                        except (asyncio.CancelledError, Exception):
                            pass
                        hash_task = None
                    continue

                if not self.cancel_flag.is_set():
                    # ── FIX 1: REAL FINALIZATION — verifies server MD5 ─────
                    if self.cfg.verify_after_upload:
                        await self.tus.finalize_upload(upload_url, full_hash)
                    # ──────────────────────────────────────────────────────
                    self.ledger.mark_completed(file_path)
                    prog = metrics.get_progress()
                    log.info(
                        "Upload complete",
                        event="upload_complete",
                        file=os.path.basename(file_path),
                        correlation_id=correlation_id,
                        mode=selected_mode,
                        **prog,
                    )
                    return True

        except asyncio.CancelledError:
            log.warning(
                "Upload cancelled cleanly",
                event="upload_cancelled",
                file=os.path.basename(file_path),
                correlation_id=correlation_id,
            )
            return False

        except (DiskFullError, FileAccessError) as exc:
            log.critical(
                f"Local hardware error: {exc}",
                event="hardware_error",
                file=os.path.basename(file_path),
                correlation_id=correlation_id,
            )
            return False

        except IntegrityError as exc:
            log.critical(
                f"Data integrity failure: {exc}",
                event="integrity_failure",
                file=os.path.basename(file_path),
                correlation_id=correlation_id,
            )
            if upload_url:
                self.ledger.update_session(file_path, upload_url, "integrity_failed")
            return False

        except AllEndpointsFailedError as exc:
            log.critical(
                f"All upload endpoints failed: {exc}",
                event="all_endpoints_failed",
                file=os.path.basename(file_path),
                correlation_id=correlation_id,
            )
            return False

        except FatalUploadError as exc:
            log.critical(
                f"Fatal upload error: {exc}",
                event="upload_fatal",
                file=os.path.basename(file_path),
                correlation_id=correlation_id,
            )
            if upload_url and not self.cancel_flag.is_set():
                self.ledger.update_session(file_path, upload_url, "failed")
            return False

        except Exception as exc:
            log.error(
                "Engine crash",
                event="engine_crash",
                file=os.path.basename(file_path),
                correlation_id=correlation_id,
                exc_info=True,
            )
          # ====================================================================
            # 🚨 ENTERPRISE SELF-HEALING LEDGER
            # Catch Webhook Timeouts, Proxy Disconnects, and Dead Sockets
            # ====================================================================
            err_str = str(exc)
            if any(e in err_str for e in ["ConnectError", "All connection attempts failed", "ReadError", "ReadTimeout", "RemoteProtocolError"]):
                log.warning(f"🔄 Network/Proxy Disconnect Detected. Wiping stale/ghost URL for {os.path.basename(file_path)}")
                try:
                    import sqlite3
                    # Use a direct, fresh connection to safely wipe the bad ledger entry
                    # Assuming your ledger is stored in a file like 'tus_ledger.db'
                    ledger_path = getattr(self.ledger, 'db_path', 'tus_ledger.db') 
                    with sqlite3.connect(ledger_path, timeout=10) as heal_conn:
                        heal_conn.execute("DELETE FROM upload_ledger WHERE file_path = ?", (file_path,))
                        heal_conn.commit()
                    log.info(f"✅ Auto-heal successful. Corrupted session wiped for {os.path.basename(file_path)}")
                except Exception as db_err:
                    log.error(f"Failed to auto-heal ledger: {db_err}")
            # ====================================================================
            
            return False

        finally:
            self._upload_in_progress = False
            if hash_task and not hash_task.done():
                hash_task.cancel()
                try:
                    await hash_task
                except (asyncio.CancelledError, Exception):
                    pass

        log.critical(
            "All session retries exhausted",
            event="session_retries_exhausted",
            file=os.path.basename(file_path),
            correlation_id=correlation_id,
        )
        return False