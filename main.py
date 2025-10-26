from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
import subprocess
import sys
import os
import tempfile
import json

app = FastAPI(
    title="YouTube -> Telegram Streaming API",
    description="Endpoints: /info, /download, /direct, /stream, /stream_video",
    version="2.0"
)

# -------------------------------
# Helper: Windows-safe yt-dlp call
# -------------------------------
def run_yt_dlp(args: list):
    cmd = [sys.executable, "-m", "yt_dlp"] + args
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"yt-dlp run failed: {result.stderr}")
    return result.stdout

# -------------------------------
# /info endpoint
# -------------------------------
@app.get("/info")
def get_info(url: str = Query(..., description="YouTube video URL")):
    try:
        output = run_yt_dlp(["--dump-json", url])
        data = json.loads(output)
        return {
            "title": data.get("title"),
            "uploader": data.get("uploader"),
            "duration": data.get("duration"),
            "view_count": data.get("view_count"),
            "webpage_url": data.get("webpage_url"),
            "thumbnail": data.get("thumbnail")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# /download endpoint (MP3)
# -------------------------------
@app.get("/download")
def download_mp3(url: str = Query(..., description="YouTube video URL")):
    try:
        temp_dir = tempfile.mkdtemp()
        out_file = os.path.join(temp_dir, "%(title)s.%(ext)s")
        run_yt_dlp([
            "--extract-audio",
            "--audio-format", "mp3",
            "--no-playlist",
            "-o", out_file,
            url
        ])
        files = os.listdir(temp_dir)
        if not files:
            raise HTTPException(status_code=500, detail="MP3 not found")
        mp3_path = os.path.join(temp_dir, files[0])
        return FileResponse(mp3_path, filename=files[0], media_type="audio/mpeg")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# /direct endpoint (best audio URL)
# -------------------------------
@app.get("/direct")
def direct_stream(url: str = Query(..., description="YouTube video URL")):
    try:
        output = run_yt_dlp([
            "-f", "bestaudio",
            "--no-playlist",
            "--get-url",
            url
        ])
        return {"direct_url": output.strip()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# /stream endpoint (for Telegram VC audio)
# -------------------------------
@app.get("/stream")
def stream(url: str = Query(None), query: str = Query(None)):
    """
    Returns a direct URL for streaming audio to Telegram VC.
    Provide either url=YOUTUBE_URL or query="song name".
    """
    try:
        if query:
            search_output = run_yt_dlp([f"ytsearch:{query}", "--get-id", "--no-playlist"])
            video_id = search_output.strip().split("\n")[0]
            url = f"https://www.youtube.com/watch?v={video_id}"

        if not url:
            raise HTTPException(status_code=400, detail="You must provide url or query")

        output = run_yt_dlp([
            "-f", "bestaudio",
            "--no-playlist",
            "--get-url",
            url
        ])
        return {"direct_url": output.strip(), "youtube_url": url}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# /stream_video endpoint (Telegram Video Chat)
# -------------------------------
@app.get("/stream_video")
def stream_video(url: str = Query(...)):
    """
    Returns a direct URL for best video/audio stream.
    """
    try:
        output = run_yt_dlp([
            "-f", "best",
            "--no-playlist",
            "--get-url",
            url
        ])
        return {"direct_url": output.strip(), "youtube_url": url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
