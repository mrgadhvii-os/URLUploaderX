import os
import yt_dlp
from config import DOWNLOAD_DIR
import time
import asyncio
from urllib.parse import urlparse
import requests
import re
import logging
from datetime import datetime
import sys
import subprocess
import shutil
from pathlib import Path
import json
import traceback
from concurrent.futures import ThreadPoolExecutor
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from typing import Callable, Optional, Tuple, Dict, Any

# Configure modern terminal logging with cleaner format
class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors"""
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    green = "\x1b[32;20m"
    blue = "\x1b[34;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    
    format_str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    FORMATS = {
        logging.DEBUG: grey + format_str + reset,
        logging.INFO: green + format_str + reset,
        logging.WARNING: yellow + format_str + reset,
        logging.ERROR: red + format_str + reset,
        logging.CRITICAL: bold_red + format_str + reset
    }
    
    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)

# Setup logger with clean output
logger = logging.getLogger("URLUploader")
logger.setLevel(logging.INFO)
logger.handlers = []  # Clear any existing handlers

# Create console handler with custom formatter
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(ColoredFormatter())
logger.addHandler(console_handler)

# Disable noisy logs from urllib3
logging.getLogger("urllib3").setLevel(logging.WARNING)

def format_bytes(bytes_val):
    """Format bytes to human readable string"""
    if bytes_val is None:
        return "0B"
    
    try:
        bytes_val = float(bytes_val)
        for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
            if abs(bytes_val) < 1024.0:
                return f"{bytes_val:3.1f}{unit}B"
            bytes_val /= 1024.0
        return f"{bytes_val:.1f}YiB"
    except:
        return "0B"

def format_time(seconds):
    """Format seconds to MM:SS"""
    if seconds is None or seconds < 0:
        return "--:--"
    
    try:
        minutes, seconds = divmod(int(seconds), 60)
        hours, minutes = divmod(minutes, 60)
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        else:
            return f"{minutes:02d}:{seconds:02d}"
    except:
        return "--:--"

class VideoInfo:
    """Class to store video metadata"""
    def __init__(self):
        self.width = 0
        self.height = 0
        self.duration = 0
        self.thumbnail = None
        self.title = None
        self.format = None

# Global event loop for callbacks
loop = asyncio.get_event_loop()

class Downloader:
    def __init__(
        self,
        url: str,
        filename: str,
        progress_callback: Optional[Callable] = None,
        download_path: str = "downloads",
    ):
        self.url = url
        self.filename = filename
        self.progress_callback = progress_callback
        self.download_path = download_path
        self.download_started = False
        self.download_canceled = False
        self.is_encrypted = False
        self.encryption_key = None
        self.video_info = VideoInfo()
        self.event_loop = loop
        self.update_interval = 3.0  # Increased to 3 seconds
        self.last_update_time = 0
        self.last_progress = 0
        self.min_size_update = 15 * 1024 * 1024  # 15MB minimum progress for update
        self.last_bytes = 0
        self.executor = ThreadPoolExecutor(max_workers=2)
        
        os.makedirs(download_path, exist_ok=True)
        
        if "*" in url:
            url_parts = url.split("*", 1)
            if len(url_parts) == 2:
                self.url = url_parts[0]
                self.encryption_key = url_parts[1]
                self.is_encrypted = True

    def decrypt_vid_data(self, vid_data, key):
        """Decrypt video data using XOR with key"""
        try:
            # Convert input to bytes if it's not already
            if isinstance(vid_data, (list, bytearray)):
                vid_data = bytes(vid_data)
            if isinstance(key, str):
                key = key.encode('utf-8')
            
            # Only decrypt the first 28 bytes (header)
            header = vid_data[:28]
            rest = vid_data[28:]
            
            # XOR decryption of header
            decrypted_header = bytearray(len(header))
            key_length = len(key)
            
            for i in range(len(header)):
                if i < key_length:
                    decrypted_header[i] = header[i] ^ key[i]
                else:
                    decrypted_header[i] = header[i] ^ i
            
            # Combine decrypted header with rest of file
            return bytes(decrypted_header) + rest
        except Exception as e:
            logger.error(f"Decryption error: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def get_file_extension(self):
        parsed_url = urlparse(self.url)
        path = parsed_url.path.lower()
        
        # Handle encrypted video URLs
        if self.encryption_key:
            # Extract original extension from URL
            ext_match = re.search(r'\.(mkv|mp4|avi|mov|wmv|flv|webm)(?:\*|$)', self.url.lower())
            if ext_match:
                return f".{ext_match.group(1)}"
            return '.mkv'  # Default for encrypted videos
        
        # Keep original extension for videos
        if any(path.endswith(ext) for ext in ['.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm']):
            ext = os.path.splitext(path)[1].lower()
            return ext if ext else '.mkv'
        
        return '.mkv'  # Default to .mkv

    async def send_initial_progress(self):
        """Send initial progress update to initialize UI"""
        if self.progress_callback:
            try:
                await self.progress_callback(0, 0, 0, 0, 0, self.filename)
                logger.info("Sent initial progress update")
            except Exception as e:
                logger.error(f"Error sending initial progress update: {e}")
                logger.error(traceback.format_exc())

    def progress_hook(self, d: Dict[str, Any]) -> None:
        """Progress hook for yt-dlp"""
        if self.download_canceled:
            raise Exception("Download was canceled")
        
        try:
            status = d.get("status")
            
            if status == "downloading":
                downloaded_bytes = d.get("downloaded_bytes", 0)
                total_bytes = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
                speed = d.get("speed", 0)
                filename = d.get("filename", "")
                
                # Only show start message once
                if not self.download_started:
                    self.download_started = True
                    logger.info("⚡ DOWNLOADING")
                
                # Calculate progress
                if total_bytes > 0:
                    progress = (downloaded_bytes / total_bytes) * 100
                else:
                    progress = 0

                # Only update terminal log if:
                # 1. At least 3 seconds have passed AND
                # 2. At least 15MB more has been downloaded
                current_time = time.time()
                bytes_progress = downloaded_bytes - self.last_bytes
                
                if (current_time - self.last_update_time >= self.update_interval and 
                    bytes_progress >= self.min_size_update):
                    
                    self.last_update_time = current_time
                    self.last_bytes = downloaded_bytes
                    
                    # Minimal terminal log
                    logger.info(f"⬇️ {progress:.1f}%")
                    
                # Always call progress callback for Telegram updates
                if self.progress_callback:
                    try:
                        coro = self.progress_callback(
                            progress, speed, total_bytes, downloaded_bytes, 
                            d.get("eta", None), filename
                        )
                        if self.event_loop and self.event_loop.is_running():
                            asyncio.run_coroutine_threadsafe(coro, self.event_loop)
                        else:
                            asyncio.run(coro)
                    except Exception:
                        pass
            
            elif status == "finished":
                logger.info("✅ COMPLETE")
                
                # Update video info silently
                if "info_dict" in d:
                    info = d["info_dict"]
                    self.video_info.title = info.get("title", "")
                    self.video_info.format = info.get("format", "")
                    
                    if "thumbnail" in info and info["thumbnail"]:
                        try:
                            thumbnail_path = os.path.join(
                                self.download_path, 
                                f"{os.path.basename(d.get('filename', 'video'))}_thumb.jpg"
                            )
                            ydl_opts = {
                                "quiet": True,
                                "no_warnings": True,
                                "outtmpl": thumbnail_path,
                            }
                            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                                ydl.download([info["thumbnail"]])
                            
                            if os.path.exists(thumbnail_path):
                                self.video_info.thumbnail = thumbnail_path
                        except Exception:
                            pass
        
        except Exception:
            pass

    async def extract_video_metadata(self, video_path):
        """Extract video metadata using ffprobe"""
        try:
            # Extract video metadata using ffprobe with more detailed format
            cmd = [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width,height,duration:format=duration",
                "-of", "json",
                video_path
            ]
            
            # Run ffprobe
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                # Parse the output
                try:
                    data = json.loads(result.stdout)
                    
                    # Try to get duration from stream first, then format
                    duration = None
                    if 'streams' in data and len(data['streams']) > 0:
                        stream = data['streams'][0]
                        self.video_info.width = int(stream.get('width', 0))
                        self.video_info.height = int(stream.get('height', 0))
                        duration = stream.get('duration')
                    
                    # If duration not in stream, try format section
                    if not duration and 'format' in data:
                        duration = data['format'].get('duration')
                    
                    # Convert duration to float if found
                    if duration:
                        self.video_info.duration = int(float(duration))
                    
                    # If still no duration, try another ffprobe command specifically for duration
                    if not self.video_info.duration:
                        duration_cmd = [
                            "ffprobe",
                            "-v", "error",
                            "-show_entries", "format=duration",
                            "-of", "default=noprint_wrappers=1:nokey=1",
                            video_path
                        ]
                        duration_result = subprocess.run(duration_cmd, capture_output=True, text=True)
                        if duration_result.returncode == 0 and duration_result.stdout.strip():
                            self.video_info.duration = int(float(duration_result.stdout))
                    
                    logger.info(
                        f"Extracted video metadata: {self.video_info.width}x{self.video_info.height}, "
                        f"Duration: {self.video_info.duration}s"
                    )
                except json.JSONDecodeError:
                    logger.warning("Could not parse ffprobe output as JSON")
                except ValueError as ve:
                    logger.warning(f"Invalid number format in metadata: {ve}")
            else:
                logger.warning(f"ffprobe failed with error: {result.stderr}")
            
            # Generate thumbnail
            thumbnail_path = os.path.join(self.download_path, f"{Path(video_path).stem}_thumb.jpg")
            cmd = [
                "ffmpeg",
                "-i", video_path,
                "-ss", "00:00:01",  # Take thumbnail from first second
                "-vframes", "1",
                "-vf", "scale=320:-1",  # 320px width, keep aspect ratio
                "-y",  # Overwrite without asking
                thumbnail_path
            ]
            
            # Run ffmpeg
            result = subprocess.run(cmd, capture_output=True)
            
            if result.returncode == 0 and os.path.exists(thumbnail_path):
                self.video_info.thumbnail = thumbnail_path
                logger.info(f"Generated thumbnail: {thumbnail_path}")
            else:
                logger.warning("Could not generate thumbnail")
        
        except Exception as e:
            logger.error(f"Error extracting video metadata: {e}")
            logger.error(traceback.format_exc())

    async def download(self) -> Tuple[bool, str, VideoInfo]:
        """Download the file with progress tracking"""
        try:
            if self.progress_callback:
                await self.send_initial_progress()
            
            output_path = os.path.join(self.download_path, self.filename)
            logger.info(f"📥 Processing: {os.path.basename(self.url)}")
            
            final_path = None
            
            if self.is_encrypted:
                logger.info("🔒 Encrypted Video Detected")
                download_success, temp_file = await self._download_with_ytdlp()
                
                if not download_success:
                    logger.error("❌ Download Failed")
                    return False, f"Download failed: {temp_file}", self.video_info
                
                logger.info("🔑 Decrypting Video...")
                try:
                    output_path = self.ensure_proper_extension(output_path)
                    with open(temp_file, "rb") as f:
                        encrypted_data = f.read()
                    
                    decrypted_data = self.decrypt_vid_data(encrypted_data, self.encryption_key)
                    
                    with open(output_path, "wb") as f:
                        f.write(decrypted_data)
                    
                    logger.info("✅ Decryption Complete")
                    final_path = output_path
                    
                    await self.extract_video_metadata(final_path)
                    
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                except Exception as e:
                    logger.error(f"❌ Decryption Failed: {str(e)}")
                    return False, f"Decryption failed: {str(e)}", self.video_info
            else:
                download_success, temp_file = await self._download_with_ytdlp()
                
                if not download_success:
                    logger.error("❌ Download Failed")
                    return False, f"Download failed: {temp_file}", self.video_info
                
                output_path = self.ensure_proper_extension(output_path)
                
                if temp_file != output_path and os.path.exists(temp_file):
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    shutil.move(temp_file, output_path)
                    logger.info("📦 File Moved to Final Location")
                
                final_path = output_path
                await self.extract_video_metadata(final_path)
            
            return True, final_path, self.video_info
        
        except Exception as e:
            logger.error(f"❌ Process Error: {str(e)}")
            return False, str(e), self.video_info

    def ensure_proper_extension(self, filepath):
        """Ensure the file has the correct extension based on the URL"""
        url_path = self.url.split("?")[0]  # Remove query params
        url_ext = os.path.splitext(url_path)[1].lower()
        
        # If URL has an extension and filepath doesn't match it
        if url_ext and not filepath.lower().endswith(url_ext):
            # Get the base filepath without extension
            base_path = os.path.splitext(filepath)[0]
            # Add the extension from URL
            return f"{base_path}{url_ext}"
        
        return filepath

    async def _download_with_ytdlp(self) -> Tuple[bool, str]:
        """Run yt-dlp download in a separate thread to avoid blocking"""
        logger.info(f"Starting yt-dlp download for {self.url}")
        
        try:
            # Send initial progress update
            if self.progress_callback and not self.download_started:
                await self.send_initial_progress()
            
            # Set up yt-dlp options
            outtmpl = os.path.join(self.download_path, "%(title).100s.%(ext)s")
            
            ydl_opts = {
                "quiet": False,
                "no_warnings": False,
                "progress_hooks": [self.progress_hook],
                "outtmpl": outtmpl,
                "format": "best/bestvideo+bestaudio",
                "writeinfojson": True,
                "retries": 10,
                "fragment_retries": 10,
                "concurrent_fragment_downloads": 32,  # Increased to 32 for more parallel downloads
                "buffersize": 8388608,  # 8MB buffer size
                "http_chunk_size": 33554432,  # 32MB chunks for faster downloads
                "throttledratelimit": 1000000000,  # 1GB/s limit
                "external_downloader": "aria2c",  # Use aria2c for faster downloads
                "external_downloader_args": [
                    "-x", "32",  # 32 connections per server
                    "-s", "32",  # Split file into 32 parts
                    "-k", "32M",  # Min split size 32MB
                    "--max-connection-per-server=32",
                    "--min-split-size=32M",
                    "--max-concurrent-downloads=32",
                    "--max-overall-download-limit=0",
                    "--max-download-limit=0",
                    "--file-allocation=none",
                    "--optimize-concurrent-downloads=true",
                    "--enable-http-keep-alive=true",
                    "--http-accept-gzip=true",
                    "--disk-cache=64M",
                    "--async-dns=true",
                    "--allow-overwrite=true",
                    "--auto-file-renaming=false"
                ],
                "geo_bypass": True,
                "no_check_certificate": True,
                "ignoreerrors": False,
                "nooverwrites": False,
                "continuedl": True,
            }
            
            # Function to run in the thread pool
            def run_download():
                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(self.url, download=True)
                        
                        # Get the actual filename that was downloaded
                        if info:
                            filename = ydl.prepare_filename(info)
                            
                            # If info_dict has resolution, update video_info
                            self.video_info.width = info.get("width", 0)
                            self.video_info.height = info.get("height", 0)
                            self.video_info.duration = info.get("duration", 0)
                            self.video_info.title = info.get("title", "")
                            
                            # Check if thumbnail info exists
                            if "thumbnail" in info and info["thumbnail"]:
                                self.video_info.thumbnail = info.get("thumbnail", "")
                            
                            return True, filename
                        return False, "Could not extract video info"
                except Exception as e:
                    logger.error(f"yt-dlp download error: {e}")
                    logger.error(traceback.format_exc())
                    return False, str(e)
            
            # Run the download in a separate thread
            result = await loop.run_in_executor(self.executor, run_download)
            return result
        
        except Exception as e:
            logger.error(f"Error setting up yt-dlp download: {e}")
            logger.error(traceback.format_exc())
            return False, str(e) 
