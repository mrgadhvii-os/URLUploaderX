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
        self.update_interval = 1.0  # Reduced to 1 second for smoother progress
        self.last_update_time = 0
        self.last_progress = 0
        self.download_finished = False
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
                
                if not self.download_started:
                    self.download_started = True
                    logger.info("⚡ DOWNLOADING")
                
                if total_bytes > 0:
                    progress = min((downloaded_bytes / total_bytes) * 100, 99.9)  # Cap at 99.9%
                else:
                    progress = 0
                
                current_time = time.time()
                if (current_time - self.last_update_time) >= self.update_interval:
                    self.last_update_time = current_time
                    self.last_progress = progress
                    
                    # Minimal terminal log
                    logger.info(f"⬇️ {progress:.1f}%")
                    
                    # Always call progress callback for UI updates
                    if self.progress_callback:
                        try:
                            coro = self.progress_callback(
                                progress, speed, total_bytes, downloaded_bytes, 
                                d.get("eta", None), filename
                            )
                            if self.event_loop and self.event_loop.is_running():
                                future = asyncio.run_coroutine_threadsafe(coro, self.event_loop)
                                future.result(timeout=1)  # 1 second timeout
                            else:
                                asyncio.run(coro)
                        except Exception:
                            pass
            
            elif status == "finished":
                if not self.download_finished:  # Prevent multiple finish notifications
                    self.download_finished = True
                    logger.info("✅ Download Complete")
                    
                    # Final progress update
                    if self.progress_callback:
                        try:
                            coro = self.progress_callback(100, 0, total_bytes, total_bytes, 0, filename)
                            if self.event_loop and self.event_loop.is_running():
                                future = asyncio.run_coroutine_threadsafe(coro, self.event_loop)
                                future.result(timeout=1)
                            else:
                                asyncio.run(coro)
                        except Exception:
                            pass
                    
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
        """Run yt-dlp download in a separate thread"""
        logger.info(f"Starting download: {self.url}")
        
        try:
            outtmpl = os.path.join(self.download_path, "%(title).100s.%(ext)s")
            
            ydl_opts = {
                "quiet": True,
                "no_warnings": True,
                "progress_hooks": [self.progress_hook],
                "outtmpl": outtmpl,
                "format": "best/bestvideo+bestaudio",
                "retries": 3,
                "fragment_retries": 3,
                "retry_sleep": lambda n: 3,  # Fixed retry delay
                "concurrent_fragment_downloads": 16,  # Good balance for high-speed
                "buffersize": 16777216,  # 16MB buffer
                "http_chunk_size": 16777216,  # 16MB chunks
                "throttledratelimit": None,  # Remove speed limit completely
                "external_downloader": "aria2c",
                "external_downloader_args": [
                    "-x", "16",  # 16 connections
                    "-s", "16",  # 16 splits
                    "-k", "16M",  # 16MB min split
                    "--max-connection-per-server=16",
                    "--min-split-size=16M",
                    "--max-concurrent-downloads=16",
                    "--max-overall-download-limit=0",  # No speed limit
                    "--max-download-limit=0",  # No speed limit
                    "--disk-cache=64M",
                    "--optimize-concurrent-downloads=true",
                    "--async-dns=true",
                    "--enable-http-pipelining=true",
                    "--file-allocation=none",
                    "--download-result=hide",
                    "--summary-interval=0",
                    "--stream-piece-selector=geom",  # Better piece selection for high speed
                    "--enable-http-keep-alive=true",
                    "--http-accept-gzip=true",
                    "--uri-selector=adaptive"  # Better URI selection for high speed
                ],
                "socket_timeout": 15,  # Increased timeout for high-speed transfers
                "no_check_certificate": True,
                "continuedl": True,
            }
            
            def run_download():
                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(self.url, download=True)
                        if info:
                            filename = ydl.prepare_filename(info)
                            self.video_info.width = info.get("width", 0)
                            self.video_info.height = info.get("height", 0)
                            self.video_info.duration = info.get("duration", 0)
                            self.video_info.title = info.get("title", "")
                            
                            # Verify file exists and is not empty
                            if os.path.exists(filename) and os.path.getsize(filename) > 0:
                                return True, filename
                            else:
                                return False, "Downloaded file is empty or missing"
                        return False, "Could not extract video info"
                except Exception as e:
                    logger.error(f"Download error: {str(e)}")
                    return False, str(e)
            
            # Run download with timeout
            result = await asyncio.wait_for(
                loop.run_in_executor(self.executor, run_download),
                timeout=3600  # 1 hour timeout
            )
            return result
            
        except asyncio.TimeoutError:
            logger.error("Download timed out after 1 hour")
            return False, "Download timed out"
        except Exception as e:
            logger.error(f"Download setup error: {str(e)}")
            return False, str(e) 
