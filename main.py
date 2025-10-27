# main.py — hardened + pydantic models + rate limiting + request IDs
from __future__ import annotations

from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    Header,
    Depends,
    BackgroundTasks,
    Request,
)
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from dotenv import load_dotenv

import subprocess
import sys
import os
import tempfile
import logging
import json
import re
import hmac
import shutil
import uuid
import time
import threading
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs
from datetime import datetime
from contextvars import ContextVar

from pymongo import MongoClient
import requests

# -------------------------------
# Logging (with request-id)
# -------------------------------
request_id_var: ContextVar[Optional[str]] = ContextVar("request_id", default=None)

class RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        rid = request_id_var.get()
        record.request_id = rid if rid else "-"
        return True

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s [rid=%(request_id)s]: %(message)s",
)
for _h in logging.getLogger().handlers:
    _h.addFilter(RequestIdFilter())

logger = logging.getLogger("YT-Telegram-API")

# -------------------------------
# Load environment variables
# -------------------------------
load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
API_KEY = os.getenv("API_KEY")
MONGO_URI = os.getenv("MONGO_URI")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL = os.getenv("TELEGRAM_CHANNEL")
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",")]

# Rate limiting env (default: 60 req / 60s; download stricter: 10 / 60s)
RL_WINDOW_SECONDS = int(os.getenv("RL_WINDOW_SECONDS", "60"))
RL_MAX_REQUESTS = int(os.getenv("RL_MAX_REQUESTS", "60"))
RL_DOWNLOAD_WINDOW = int(os.getenv("RL_DOWNLOAD_WINDOW", "60"))
RL_DOWNLOAD_MAX = int(os.getenv("RL_DOWNLOAD_MAX", "10"))

if not all([YOUTUBE_API_KEY, API_KEY, MONGO_URI, TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL]):
    raise RuntimeError(
        "Missing environment variables. Need YOUTUBE_API_KEY, API_KEY, MONGO_URI, TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL"
    )

logger.info("Environment loaded.")

# -------------------------------
# MongoDB Client (+ TTL)
# -------------------------------
mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=4000)
db = mongo["yt_stream"]
collection = db["songs"]

try:
    collection.create_index("created_at", expireAfterSeconds=86400)  # 1 day TTL
    logger.info("Mongo connected and TTL index ensured.")
except Exception as idx_err:
    logger.warning(f"TTL index creation failed: {idx_err}")

# -------------------------------
# YouTube API client (lazy)
# -------------------------------
_youtube_client = None

def get_youtube_client():
    global _youtube_client
    if _youtube_client is None:
        _youtube_client = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
        logger.info("YouTube API client initialized.")
    return _youtube_client

# -------------------------------
# FastAPI App
# -------------------------------
app = FastAPI(
    title="YouTube -> Telegram Streaming API",
    description="Endpoints: /health, /yt_search, /info, /stream, /download",
    version="1.4-hardened",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------
# Request-ID middleware (+ simple access log)
# -------------------------------
@app.middleware("http")
async def add_request_id(request: Request, call_next):
    rid = request.headers.get("X-Request-ID") or uuid.uuid4().hex
    token = request_id_var.set(rid)
    start = time.perf_counter()
    try:
        response = await call_next(request)
    finally:
        duration_ms = (time.perf_counter() - start) * 1000.0
        logger.info(f'{request.client.host if request.client else "?"} '
                    f'{request.method} {request.url.path} -> {response.status_code} '
                    f'in {duration_ms:.1f}ms')
        request_id_var.reset(token)
    response.headers["X-Request-ID"] = rid
    return response

# -------------------------------
# Pydantic models (responses)
# -------------------------------
from pydantic import BaseModel, Field

class HealthModel(BaseModel):
    status: str = Field(..., example="ok")
    service: str = Field(..., example="yt-telegram-api")
    version: str = Field(..., example="1.4-hardened")

class SearchItem(BaseModel):
    title: str
    videoId: Optional[str] = None
    url: str

class SearchResponse(BaseModel):
    results: List[SearchItem]

class InfoResponse(BaseModel):
    title: str
    uploader: str
    duration: str
    view_count: Optional[str] = None
    webpage_url: str
    thumbnail: Optional[str] = None

class StreamResponse(BaseModel):
    direct_url: str
    youtube_url: str

class ErrorResponse(BaseModel):
    detail: str
    request_id: Optional[str] = None

# -------------------------------
# Rate limiting (in-memory)
# NOTE: for multi-process/production, use Redis or similar shared store.
# -------------------------------
_lock = threading.Lock()
_buckets: Dict[Tuple[str, str], Deque[float]] = {}

def _limiter(max_requests: int, window_seconds: int):
    def dependency(request: Request):
        now = time.monotonic()
        ip = request.client.host if request.client else "unknown"
        key = (ip, request.url.path)
        cutoff = now - window_seconds
        with _lock:
            q = _buckets.setdefault(key, deque())
            # drop old timestamps
            while q and q[0] < cutoff:
                q.popleft()
            if len(q) >= max_requests:
                logger.warning(f"Rate limit exceeded for {ip} {request.url.path}")
                raise HTTPException(status_code=429, detail="Too Many Requests")
            q.append(now)
        return True
    return dependency

rate_limit_default = _limiter(RL_MAX_REQUESTS, RL_WINDOW_SECONDS)
rate_limit_download = _limiter(RL_DOWNLOAD_MAX, RL_DOWNLOAD_WINDOW)

# -------------------------------
# Helpers
# -------------------------------
def run_yt_dlp(args: List[str], timeout: int = 45) -> str:
    """Run yt-dlp via the current Python interpreter. Returns stdout (stripped)."""
    base = [sys.executable, "-m", "yt_dlp", "--ignore-config", "--no-cache-dir", "--no-warnings", "-q"]
    cmd = base + args
    logger.info(f"Running yt-dlp: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        logger.error("yt-dlp timed out")
        raise HTTPException(status_code=504, detail="yt-dlp timed out")

    if result.returncode != 0:
        logger.error(f"yt-dlp failed.\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
        # Don't leak stderr to clients
        raise HTTPException(status_code=502, detail="yt-dlp failed")

    logger.info("yt-dlp succeeded.")
    return result.stdout.strip()

def send_to_telegram(file_path: str, caption: str | None = None):
    """Send audio file to Telegram channel, enforce ~50MB limit."""
    file_size = os.path.getsize(file_path)
    logger.info(f"Sending to Telegram: {file_path} ({file_size} bytes)")

    if file_size > 50 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File size exceeds 50MB limit")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendAudio"
    with open(file_path, "rb") as f:
        files = {"audio": f}
        data = {"chat_id": TELEGRAM_CHANNEL, "caption": caption or ""}
        r = requests.post(url, files=files, data=data, timeout=60)

    if not r.ok:
        logger.error(f"Telegram upload failed: {r.status_code} {r.text}")
        raise HTTPException(status_code=502, detail="Telegram upload failed")

    logger.info("Telegram upload OK.")
    return r.json()

def extract_video_id(url: str) -> str | None:
    """Extract a YouTube video ID from various URL formats."""
    try:
        u = urlparse(url)
        # youtu.be/<id>
        if u.netloc.endswith("youtu.be"):
            vid = u.path.lstrip("/")
            return vid or None
        # youtube or music.youtube watch?v=<id>
        if u.netloc.endswith(("youtube.com", "www.youtube.com", "music.youtube.com")) and "watch" in u.path:
            q = parse_qs(u.query)
            vid = q.get("v", [None])[0]
            if vid:
                return vid
        # shorts or embed
        m = re.search(r"/(shorts|embed)/([A-Za-z0-9_-]{6,})", u.path)
        if m:
            return m.group(2)
    except Exception:
        return None
    return None

# -------------------------------
# Auth dependency (timing-safe)
# -------------------------------
def get_api_key(
    api_key: str | None = Header(None, alias="api-key"),
    x_api_key: str | None = Header(None, alias="x-api-key"),
    authorization: str | None = Header(None, alias="Authorization"),
):
    """Allow api-key, x-api-key, or Authorization: Bearer <token>."""
    token = None
    if api_key:
        token = api_key
    elif x_api_key:
        token = x_api_key
    elif authorization and authorization.startswith("Bearer "):
        token = authorization.replace("Bearer ", "", 1)

    if not (token and hmac.compare_digest(token, API_KEY)):
        logger.warning("Auth failed: provided key does not match.")
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return True

# -------------------------------
# YouTube search (Data API with yt-dlp fallback)
# -------------------------------
def youtube_search(query: str, max_results: int = 5):
    logger.info(f"youtube_search(query={query!r}, max_results={max_results})")

    # Try official Data API first
    try:
        yt = get_youtube_client()
        request = yt.search().list(part="snippet", q=query, maxResults=max_results, type="video")
        response = request.execute()
        items = response.get("items", [])
        results = [
            {
                "title": it["snippet"]["title"],
                "videoId": it["id"]["videoId"],
                "url": f"https://www.youtube.com/watch?v={it['id']['videoId']}",
            }
            for it in items
        ]
        if results:
            return results
        logger.info("YouTube Data API returned 0 items, will try yt-dlp fallback.")
    except HttpError as e:
        logger.error(f"YouTube API error: {e}")
        # fall through to yt-dlp fallback
    except Exception as e:
        logger.error(f"YouTube API unexpected error: {e}")
        # fall through to yt-dlp fallback

    # Fallback: yt-dlp search (no quota needed)
    try:
        out = run_yt_dlp(["--dump-json", f"ytsearch{max_results}:{query}"])
        results = []
        for line in out.splitlines():
            try:
                j = json.loads(line)
                if j.get("webpage_url") and j.get("title"):
                    vid = j.get("id")
                    if not vid and j.get("webpage_url"):
                        vid = extract_video_id(j["webpage_url"]) or ""
                    results.append({"title": j.get("title"), "videoId": vid, "url": j.get("webpage_url")})
            except json.JSONDecodeError:
                continue
        logger.info(f"yt-dlp fallback returned {len(results)} items")
        return results
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"yt-dlp fallback failed: {e}")
        return []

# -------------------------------
# Error handlers (include request_id)
# -------------------------------
@app.exception_handler(HTTPException)
async def http_exc_handler(request: Request, exc: HTTPException):
    rid = request.headers.get("X-Request-ID") or request_id_var.get()
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail if exc.detail else "HTTP error", "request_id": rid},
    )

@app.exception_handler(Exception)
async def unhandled_exc(request: Request, exc: Exception):
    logger.exception("Unhandled error")
    rid = request.headers.get("X-Request-ID") or request_id_var.get()
    return JSONResponse(status_code=500, content={"detail": "Internal error", "request_id": rid})

# -------------------------------
# Routes
# -------------------------------
@app.get("/health", response_model=HealthModel, responses={429: {"model": ErrorResponse}})
def health(_rl: bool = Depends(rate_limit_default)):
    return {"status": "ok", "service": "yt-telegram-api", "version": "1.4-hardened"}

@app.get(
    "/yt_search",
    response_model=SearchResponse,
    responses={429: {"model": ErrorResponse}, 403: {"model": ErrorResponse}},
)
def yt_search_endpoint(
    query: str = Query(..., max_length=200, description="Search query for YouTube"),
    limit: int = Query(5, ge=1, le=20, description="Max results (1-20)"),
    _ok: bool = Depends(get_api_key),
    _rl: bool = Depends(rate_limit_default),
):
    try:
        results = youtube_search(query, max_results=limit)
        return {"results": results}
    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected error in /yt_search")
        raise HTTPException(status_code=500, detail="Internal error")

@app.get(
    "/info",
    response_model=InfoResponse,
    responses={400: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 429: {"model": ErrorResponse}},
)
def get_info(
    url: str = Query(..., description="YouTube URL (watch, youtu.be, shorts supported)"),
    _ok: bool = Depends(get_api_key),
    _rl: bool = Depends(rate_limit_default),
):
    try:
        vid = extract_video_id(url)
        if not vid:
            q = parse_qs(urlparse(url).query)
            vid = (q.get("v", [None])[0])
        if not vid:
            raise HTTPException(status_code=400, detail="Could not extract video id")

        yt = get_youtube_client()
        request = yt.videos().list(part="snippet,contentDetails,statistics", id=vid)
        response = request.execute()
        items = response.get("items", [])
        if not items:
            raise HTTPException(status_code=404, detail="Video not found")
        item = items[0]

        thumbs = item["snippet"].get("thumbnails", {})
        thumb_url = (
            (thumbs.get("high") or {}).get("url")
            or (thumbs.get("medium") or {}).get("url")
            or (thumbs.get("default") or {}).get("url")
        )

        return {
            "title": item["snippet"]["title"],
            "uploader": item["snippet"]["channelTitle"],
            "duration": item["contentDetails"]["duration"],
            "view_count": item["statistics"].get("viewCount"),
            "webpage_url": url,
            "thumbnail": thumb_url,
        }
    except HttpError as e:
        logger.error(f"YouTube API error in /info: {e}")
        raise HTTPException(status_code=502, detail="YouTube API error")
    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected error in /info")
        raise HTTPException(status_code=500, detail="Internal error")

@app.get(
    "/stream",
    response_model=StreamResponse,
    responses={400: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 429: {"model": ErrorResponse}},
)
def stream(
    url: str | None = Query(None, description="Full YouTube URL"),
    query: str | None = Query(None, max_length=200, description="Search on YouTube and take first result"),
    _ok: bool = Depends(get_api_key),
    _rl: bool = Depends(rate_limit_default),
):
    try:
        if query and not url:
            search_res = youtube_search(query, max_results=1)
            if not search_res:
                raise HTTPException(status_code=404, detail="No video found")
            url = search_res[0]["url"]

        if not url:
            raise HTTPException(status_code=400, detail="You must provide url or query")

        output = run_yt_dlp([
            "-f", "bestaudio",
            "--no-playlist",
            "--get-url",
            url,
        ])

        try:
            collection.insert_one({"url": url, "direct_url": output, "created_at": datetime.utcnow()})
        except Exception as mongo_err:
            logger.error(f"Mongo insert failed: {mongo_err}")

        return {"direct_url": output, "youtube_url": url}
    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected error in /stream")
        raise HTTPException(status_code=500, detail="Internal error")

@app.get(
    "/download",
    responses={
        400: {"model": ErrorResponse},
        429: {"model": ErrorResponse},
        502: {"model": ErrorResponse},
        504: {"model": ErrorResponse},
    },
)
def download_mp3(
    url: str = Query(..., description="Full YouTube URL"),
    _ok: bool = Depends(get_api_key),
    _rl: bool = Depends(rate_limit_download),
    bg: BackgroundTasks = None,
):
    temp_dir = None
    try:
        temp_dir = tempfile.mkdtemp(prefix="ytmp3_")
        out_file = os.path.join(temp_dir, "%(title)s.%(ext)s")

        run_yt_dlp([
            "--extract-audio",
            "--audio-format", "mp3",
            "--audio-quality", "5",         # ~130kbps to help stay under 50MB
            "--max-filesize", "49M",        # fail early if likely to exceed Telegram limit
            "--no-playlist",
            "-o", out_file,
            url,
        ])

        files = os.listdir(temp_dir)
        if not files:
            raise HTTPException(status_code=500, detail="MP3 not found after download")
        mp3_name = files[0]
        mp3_path = os.path.join(temp_dir, mp3_name)

        # Send to Telegram (enforces 50MB)
        send_to_telegram(mp3_path, caption=mp3_name)

        # cleanup after response is sent
        if bg:
            bg.add_task(lambda p=temp_dir: shutil.rmtree(p, ignore_errors=True))

        return FileResponse(mp3_path, filename=mp3_name, media_type="audio/mpeg")
    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected error in /download")
        try:
            if temp_dir:
                shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception:
            pass
        raise HTTPException(status_code=500, detail="Internal error")

# Optional: run with `python main.py` during local dev
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=7000, reload=True)
