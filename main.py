# main.py
from fastapi import FastAPI, HTTPException, Query, Header
from dotenv import load_dotenv
from googleapiclient.discovery import build
from pymongo import MongoClient
from datetime import datetime
from pyrogram import Client
import tempfile, subprocess, sys, os, asyncio
from urllib.parse import urlparse, parse_qs

# -------------------------------
# Load environment variables
# -------------------------------
load_dotenv()
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("DB_NAME")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")

if not all([YOUTUBE_API_KEY, MONGODB_URI, DB_NAME, TELEGRAM_BOT_TOKEN, CHANNEL_ID]):
    raise RuntimeError("Environment variables missing. Check .env file")

# -------------------------------
# Initialize YouTube API
# -------------------------------
youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

# -------------------------------
# Connect MongoDB
# -------------------------------
client = MongoClient(MONGODB_URI)
db = client[DB_NAME]
searches_col = db['searches']
downloads_col = db['downloads']
bots_col = db['bots']  # Store multiple bot keys

# -------------------------------
# Initialize Telegram client
# -------------------------------
app_telegram = Client("music_bot", bot_token=TELEGRAM_BOT_TOKEN)

async def upload_to_channel(file_path, caption=""):
    async with app_telegram:
        msg = await app_telegram.send_audio(
            chat_id=CHANNEL_ID,
            audio=file_path,
            caption=caption
        )
        return msg.file_id

# -------------------------------
# FastAPI App
# -------------------------------
app = FastAPI(
    title="YouTube -> Telegram Streaming API",
    description="Endpoints: /yt_search, /info, /stream, /download",
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
# Helper: Extract video ID
# -------------------------------
def extract_video_id(url: str):
    parsed = urlparse(url)
    if parsed.hostname in ["youtu.be"]:
        return parsed.path[1:]
    if parsed.hostname in ["www.youtube.com", "youtube.com"]:
        return parse_qs(parsed.query).get("v", [None])[0]
    return None

# -------------------------------
# Helper: Check bot API key
# -------------------------------
def verify_bot_key(api_key: str):
    if not api_key:
        raise HTTPException(status_code=403, detail="API key required")
    bot = bots_col.find_one({"api_key": api_key})
    if not bot:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return True

# -------------------------------
# /yt_search endpoint
# -------------------------------
@app.get("/yt_search")
def yt_search(query: str = Query(...), api_key: str = Header(None)):
    verify_bot_key(api_key)
    try:
        cached = searches_col.find_one({"query": query})
        if cached:
            return {"results": cached["results"]}

        request = youtube.search().list(
            part="snippet",
            q=query,
            maxResults=5,
            type="video"
        )
        response = request.execute()
        results = [{
            "title": item["snippet"]["title"],
            "videoId": item["id"]["videoId"],
            "url": f"https://www.youtube.com/watch?v={item['id']['videoId']}"
        } for item in response.get("items", [])]

        searches_col.insert_one({
            "query": query,
            "results": results,
            "timestamp": datetime.utcnow()
        })

        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# /info endpoint
# -------------------------------
@app.get("/info")
def get_info(url: str = Query(...), api_key: str = Header(None)):
    verify_bot_key(api_key)
    try:
        video_id = extract_video_id(url)
        if not video_id:
            raise HTTPException(status_code=400, detail="Invalid YouTube URL")

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
# /stream endpoint (Telegram streaming + caching)
# -------------------------------
@app.get("/stream")
async def stream(url: str = Query(None), query: str = Query(None), api_key: str = Header(None)):
    verify_bot_key(api_key)
    try:
        # Search if query given
        if query:
            search_res = yt_search(query, api_key=api_key)
            if not search_res["results"]:
                raise HTTPException(status_code=404, detail="No video found")
            url = search_res["results"][0]["url"]

        if not url:
            raise HTTPException(status_code=400, detail="You must provide url or query")

        # Check DB cache
        cached = downloads_col.find_one({"video_url": url})
        if cached:
            return {"telegram_file_id": cached["telegram_file_id"], "title": cached["title"], "youtube_url": url}

        # Download MP3
        with tempfile.TemporaryDirectory() as temp_dir:
            out_file = os.path.join(temp_dir, "%(title)s.%(ext)s")
            run_yt_dlp([
                "--extract-audio",
                "--audio-format", "mp3",
                "--no-playlist",
                "-o", out_file,
                url
            ])
            mp3_path = os.path.join(temp_dir, os.listdir(temp_dir)[0])
            title = os.path.basename(mp3_path).replace(".mp3", "")

            # Upload to Telegram channel
            telegram_file_id = await upload_to_channel(mp3_path, caption=title)

            # Save to DB
            downloads_col.insert_one({
                "video_url": url,
                "title": title,
                "telegram_file_id": telegram_file_id,
                "timestamp": datetime.utcnow()
            })

        return {"telegram_file_id": telegram_file_id, "title": title, "youtube_url": url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# /download endpoint (alias for /stream)
# -------------------------------
@app.get("/download")
async def download_mp3(url: str = Query(...), api_key: str = Header(None)):
    return await stream(url=url, api_key=api_key)
