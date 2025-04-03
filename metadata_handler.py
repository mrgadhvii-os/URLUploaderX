import os
import asyncio
import json
import logging
from PIL import Image
import subprocess
import re
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VideoMetadata:
    def __init__(self, file_path):
        self.file_path = file_path
        self.width = None
        self.height = None
        self.duration = None
        self.thumbnail = None
        self.is_valid = False

async def process_video(video_path, force_generate=False):
    """Process video file to extract metadata and generate thumbnail"""
    try:
        logger.info(f"Processing video: {video_path}")
        metadata = VideoMetadata(video_path)
        
        # Wait for file to be fully written/decrypted
        if '*' in video_path:  # For encrypted videos
            logger.info("Encrypted video detected, waiting for decryption...")
            await asyncio.sleep(8)
        
        # Ensure file exists and is not empty
        if not os.path.exists(video_path) or os.path.getsize(video_path) == 0:
            logger.error(f"Video file not found or empty: {video_path}")
            return metadata
        
        # Wait for file size to stabilize
        file_size = 0
        new_size = os.path.getsize(video_path)
        while new_size != file_size:
            file_size = new_size
            await asyncio.sleep(1)
            if os.path.exists(video_path):
                new_size = os.path.getsize(video_path)
            else:
                logger.error("Video file disappeared during processing")
                return metadata
                
        logger.info(f"Video file stabilized at size: {file_size} bytes")
        
        # Extract metadata using ffprobe
        try:
            cmd = [
                'ffprobe',
                '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'stream=width,height,duration,r_frame_rate',
                '-show_entries', 'format=duration',
                '-of', 'json',
                video_path
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            if process.returncode == 0:
                probe = json.loads(stdout.decode())
                stream_info = probe.get('streams', [{}])[0]
                format_info = probe.get('format', {})
                
                # Get duration from multiple sources
                duration_sources = [
                    float(stream_info.get('duration', 0)),
                    float(format_info.get('duration', 0))
                ]
                metadata.duration = int(max(d for d in duration_sources if d > 0))
                
                # Get dimensions
                metadata.width = int(stream_info.get('width', 1280))
                metadata.height = int(stream_info.get('height', 720))
                
                # Scale dimensions if needed
                if metadata.width < 1280:
                    scale = 1280 / metadata.width
                    metadata.width = 1280
                    metadata.height = int(metadata.height * scale)
                
                # Ensure even dimensions
                metadata.width = metadata.width // 2 * 2
                metadata.height = metadata.height // 2 * 2
                
                logger.info(f"Extracted metadata: {metadata.width}x{metadata.height}, {metadata.duration}s")
            
        except Exception as e:
            logger.error(f"Error extracting metadata with ffprobe: {e}")
            # Fallback to ffmpeg
            try:
                cmd = [
                    'ffmpeg',
                    '-i', video_path,
                    '-f', 'null',
                    '-'
                ]
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                _, stderr = await process.communicate()
                stderr = stderr.decode()
                
                # Extract duration
                duration_match = re.search(r"Duration: (\d{2}):(\d{2}):(\d{2})", stderr)
                if duration_match:
                    h, m, s = map(int, duration_match.groups())
                    metadata.duration = h * 3600 + m * 60 + s
                
                # Extract dimensions
                dim_match = re.search(r"Stream.*Video.* (\d+)x(\d+)", stderr)
                if dim_match:
                    metadata.width = int(dim_match.group(1))
                    metadata.height = int(dim_match.group(2))
            except Exception as e:
                logger.error(f"Error extracting metadata with ffmpeg: {e}")
        
        # Generate thumbnail
        if force_generate or not metadata.thumbnail:
            thumbnail_path = await generate_thumbnail(video_path)
            if thumbnail_path:
                metadata.thumbnail = thumbnail_path
                logger.info(f"Generated thumbnail: {thumbnail_path}")
        
        metadata.is_valid = bool(
            metadata.width and 
            metadata.height and 
            metadata.duration and 
            metadata.thumbnail
        )
        
        return metadata
        
    except Exception as e:
        logger.error(f"Error processing video: {e}")
        return metadata

async def generate_thumbnail(video_path, max_attempts=5):
    """Generate thumbnail from video with multiple attempts"""
    try:
        thumbnail_path = f"{video_path}_thumb.jpg"
        base_timestamp = 1
        
        # Ensure video file is accessible
        if not os.path.exists(video_path):
            logger.error(f"Video file not found: {video_path}")
            return None
            
        # Wait for video file to be fully written
        file_size = 0
        new_size = os.path.getsize(video_path)
        while new_size != file_size:
            file_size = new_size
            await asyncio.sleep(1)
            if os.path.exists(video_path):
                new_size = os.path.getsize(video_path)
            else:
                logger.error("Video file disappeared during size check")
                return None
        
        logger.info(f"Video file stabilized at size: {file_size} bytes")
        
        # Remove existing thumbnail if any
        if os.path.exists(thumbnail_path):
            try:
                os.remove(thumbnail_path)
                logger.info("Removed existing thumbnail")
            except Exception as e:
                logger.error(f"Error removing existing thumbnail: {e}")
        
        for attempt in range(max_attempts):
            try:
                # Try different timestamps
                current_ts = base_timestamp + (attempt * 5)
                logger.info(f"Attempting thumbnail generation at timestamp {current_ts}s")
                
                # First try with seeking to keyframe
                cmd = [
                    'ffmpeg',
                    '-ss', str(current_ts),
                    '-i', video_path,
                    '-vframes', '1',
                    '-vf', 'scale=320:320:force_original_aspect_ratio=decrease',
                    '-y',
                    thumbnail_path
                ]
                
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                stdout, stderr = await process.communicate()
                stderr_text = stderr.decode()
                
                # Check if thumbnail was generated
                if os.path.exists(thumbnail_path) and os.path.getsize(thumbnail_path) > 0:
                    try:
                        # Verify and optimize thumbnail
                        with Image.open(thumbnail_path) as img:
                            # Convert to RGB if needed
                            if img.mode != 'RGB':
                                img = img.convert('RGB')
                            # Create a new image with white background
                            bg = Image.new('RGB', img.size, (255, 255, 255))
                            if img.mode == 'RGBA':
                                bg.paste(img, mask=img.split()[3])
                            else:
                                bg.paste(img)
                            # Resize maintaining aspect ratio
                            bg.thumbnail((320, 320), Image.Resampling.LANCZOS)
                            # Save with optimization
                            bg.save(thumbnail_path, "JPEG", quality=95, optimize=True)
                        logger.info(f"Successfully generated thumbnail at {current_ts}s")
                        return thumbnail_path
                    except Exception as e:
                        logger.error(f"Error processing thumbnail image: {e}")
                        # Try alternative method if image processing fails
                        continue
                
                # If first attempt fails, try alternative method
                logger.info("Trying alternative thumbnail method...")
                cmd = [
                    'ffmpeg',
                    '-i', video_path,
                    '-ss', str(current_ts),
                    '-vframes', '1',
                    '-vf', 'scale=320:320:force_original_aspect_ratio=decrease',
                    '-y',
                    thumbnail_path
                ]
                
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                await process.communicate()
                
                if os.path.exists(thumbnail_path) and os.path.getsize(thumbnail_path) > 0:
                    try:
                        with Image.open(thumbnail_path) as img:
                            if img.mode != 'RGB':
                                img = img.convert('RGB')
                            bg = Image.new('RGB', img.size, (255, 255, 255))
                            if img.mode == 'RGBA':
                                bg.paste(img, mask=img.split()[3])
                            else:
                                bg.paste(img)
                            bg.thumbnail((320, 320), Image.Resampling.LANCZOS)
                            bg.save(thumbnail_path, "JPEG", quality=95, optimize=True)
                        logger.info(f"Successfully generated thumbnail using alternative method at {current_ts}s")
                        return thumbnail_path
                    except Exception as e:
                        logger.error(f"Error processing thumbnail image (alternative method): {e}")
                
            except Exception as e:
                logger.error(f"Thumbnail generation attempt {attempt + 1} failed: {e}")
                if attempt == max_attempts - 1:
                    logger.error("All thumbnail generation attempts failed")
                    return None
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
        
        logger.error("Failed to generate thumbnail after all attempts")
        return None
        
    except Exception as e:
        logger.error(f"Error in thumbnail generation: {e}")
        return None

def format_duration(seconds):
    """Format duration in HH:MM:SS format"""
    try:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60
        
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        else:
            return f"{minutes:02d}:{seconds:02d}"
    except:
        return "00:00"

async def ensure_video_metadata(video_path):
    """Ensure video has proper metadata and thumbnail"""
    try:
        # Process video
        metadata = await process_video(video_path, force_generate=True)
        
        if not metadata.is_valid:
            logger.warning(f"Invalid metadata for video: {video_path}")
            return None
            
        return {
            'width': metadata.width,
            'height': metadata.height,
            'duration': metadata.duration,
            'duration_text': format_duration(metadata.duration),
            'thumbnail': metadata.thumbnail,
            'is_valid': metadata.is_valid
        }
        
    except Exception as e:
        logger.error(f"Error ensuring video metadata: {e}")
        return None 