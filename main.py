from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
import subprocess, sys, os, tempfile, json
from dotenv import load_dotenv
from googleapiclient.discovery import build

# -------------------------------
# Load environment variables
# -------------------------------
load_dotenv()
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# -------------------------------
# Initialize YouTube API client
# -------------------------------
youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

# -------------------------------
# FastAPI App
# -------------------------------
app = FastAPI(
    title="YouTube -> Telegram Streaming API",
    description="Endpoints: /info, /download, /direct, /stream, /stream_video, /yt_search",
    version="3.0"
)

# -------------------------------
# Helper: yt-dlp call
# -------------------------------
def run_yt_dlp(args: list):
    cmd = [sys.executable, "-m", "yt_dlp"] + args
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"yt-dlp run failed: {result.stderr}")
    return result.stdout

# -------------------------------
# /yt_search endpoint
# -------------------------------
@app.get("/yt_search")
def yt_search(query: str = Query(..., description="Search query for YouTube")):
    try:
        request = youtube.search().list(
            part="snippet",
            q=query,
            maxResults=5,
            type="video"
        )
        response = request.execute()
        results = []
        for item in response.get("items", []):
            results.append({
                "title": item["snippet"]["title"],
                "videoId": item["id"]["videoId"],
                "url": f"https://www.youtube.com/watch?v={item['id']['videoId']}"
            })
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# /info endpoint (YouTube API)
# -------------------------------
@app.get("/info")
def get_info(url: str = Query(..., description="YouTube video URL")):
    try:
        video_id = url.split("v=")[-1]
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        if not response["items"]:
            raise HTTPException(status_code=404, detail="Video not found")
        item = response["items"][0]
        return {
            "title": item["snippet"]["title"],
            "uploader": item["snippet"]["channelTitle"],
            "duration": item["contentDetails"]["duration"],
            "view_count": item["statistics"].get("viewCount"),
            "webpage_url": url,
            "thumbnail": item["snippet"]["thumbnails"]["high"]["url"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# /stream endpoint (search or direct)
# -------------------------------
@app.get("/stream")
def stream(url: str = Query(None), query: str = Query(None)):
    try:
        if query:
            # Search using YouTube API
            search_res = yt_search(query)
            if not search_res["results"]:
                raise HTTPException(status_code=404, detail="No video found")
            url = search_res["results"][0]["url"]

        if not url:
            raise HTTPException(status_code=400, detail="You must provide url or query")

        # Get direct audio URL using yt-dlp
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
