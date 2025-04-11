import asyncio
import time
from datetime import datetime
import logging
from typing import Dict, Any, Optional, Callable, Union
from pyrogram.types import Message

# Configure logging
logger = logging.getLogger("ServerHealth")
logger.setLevel(logging.INFO)

class ServerHealthManager:
    def __init__(self):
        # Default cooldown settings
        self.default_cooldown = 60  # seconds
        self.active_cooldowns: Dict[int, Dict[str, Any]] = {}
        self.is_enabled = True
        self.last_upload_time = 0
        self.uploads_per_hour_limit = 60
        self.upload_count_hour = 0
        self.hour_start_time = time.time()
    
    def enable(self):
        """Enable health management"""
        self.is_enabled = True
        logger.info("Server Health Management: ENABLED")
    
    def disable(self):
        """Disable health management"""
        self.is_enabled = False
        logger.info("Server Health Management: DISABLED")
    
    def set_cooldown(self, seconds: int):
        """Change the default cooldown time"""
        self.default_cooldown = seconds
        logger.info(f"Server Health: Cooldown set to {seconds} seconds")
    
    def get_cooldown(self) -> int:
        """Get current cooldown setting"""
        return self.default_cooldown
    
    def is_cooling_down(self, user_id: int) -> bool:
        """Check if the specified user has an active cooldown"""
        if not self.is_enabled:
            return False
            
        if user_id in self.active_cooldowns:
            cooldown_data = self.active_cooldowns[user_id]
            if time.time() < cooldown_data['end_time']:
                return True
        return False
    
    def get_remaining_cooldown(self, user_id: int) -> int:
        """Get remaining cooldown time in seconds for a user"""
        if not self.is_enabled or user_id not in self.active_cooldowns:
            return 0
            
        cooldown_data = self.active_cooldowns[user_id]
        remaining = cooldown_data['end_time'] - time.time()
        return max(0, int(remaining))
    
    def start_cooldown(self, user_id: int, custom_time: Optional[int] = None) -> int:
        """Start a cooldown period for a user"""
        if not self.is_enabled:
            return 0
            
        cooldown_time = custom_time if custom_time is not None else self.default_cooldown
        end_time = time.time() + cooldown_time
        
        self.active_cooldowns[user_id] = {
            'start_time': time.time(),
            'end_time': end_time,
            'duration': cooldown_time
        }
        
        logger.info(f"Server Health: Started {cooldown_time}s cooldown for user {user_id}")
        return cooldown_time
    
    def clear_cooldown(self, user_id: int):
        """Clear active cooldown for a user"""
        if user_id in self.active_cooldowns:
            del self.active_cooldowns[user_id]
            logger.info(f"Server Health: Cleared cooldown for user {user_id}")
    
    def track_upload(self):
        """Track upload to enforce hourly limits"""
        current_time = time.time()
        
        # Reset counter if an hour has passed
        if current_time - self.hour_start_time >= 3600:
            self.hour_start_time = current_time
            self.upload_count_hour = 0
            
        self.upload_count_hour += 1
        self.last_upload_time = current_time
    
    def should_throttle(self) -> bool:
        """Check if uploads should be throttled based on hourly limits"""
        if not self.is_enabled:
            return False
            
        return self.upload_count_hour >= self.uploads_per_hour_limit
    
    async def display_cooldown_message(
        self, 
        client, 
        chat_id: int, 
        user_id: int,
        current_file: int,
        total_files: int,
        custom_time: Optional[int] = None,
        reply_to_message_id: Optional[int] = None
    ) -> Optional[Message]:
        """Display a cooldown message with timer"""
        cooldown_time = self.start_cooldown(user_id, custom_time)
        
        if cooldown_time <= 0:
            return None
            
        progress_percentage = (current_file / total_files) * 100
        
        message = await client.send_message(
            chat_id=chat_id,
            text=(
                f"⚠️ **SERVER HEALTH MODE** ⚠️\n\n"
                f"⏳ Cooling down for **{cooldown_time}** seconds to maintain server health...\n\n"
                f"📊 Progress: {current_file}/{total_files} files ({progress_percentage:.1f}%)\n"
                f"⏱️ Time remaining: {cooldown_time}s\n\n"
                f"_This helps prevent server overload during batch processing._"
            ),
            reply_to_message_id=reply_to_message_id
        )
        
        # Update message with countdown
        start_time = time.time()
        update_interval = 5  # Update every 5 seconds
        last_update = start_time
        
        while time.time() - start_time < cooldown_time:
            await asyncio.sleep(1)
            
            # Only update message every few seconds to avoid rate limits
            current_time = time.time()
            if current_time - last_update >= update_interval:
                last_update = current_time
                remaining = int(cooldown_time - (current_time - start_time))
                
                try:
                    await message.edit_text(
                        f"⚠️ **SERVER HEALTH MODE** ⚠️\n\n"
                        f"⏳ Cooling down for **{remaining}** seconds to maintain server health...\n\n"
                        f"📊 Progress: {current_file}/{total_files} files ({progress_percentage:.1f}%)\n"
                        f"⏱️ Time remaining: {remaining}s\n\n"
                        f"_This helps prevent server overload during batch processing._"
                    )
                except Exception as e:
                    logger.error(f"Error updating cooldown message: {e}")
        
        # Delete the message after cooldown
        try:
            await message.delete()
        except Exception as e:
            logger.error(f"Error deleting cooldown message: {e}")
            
        self.clear_cooldown(user_id)
        return None

# Create a singleton instance
health_manager = ServerHealthManager()
logger.info("Server Health Manager initialized with cooldown: %s seconds", health_manager.default_cooldown)

# Print health manager methods for debugging
logger.info("Available health manager methods:")
logger.info(" - enable() - Enable health management")
logger.info(" - disable() - Disable health management")
logger.info(" - set_cooldown(seconds) - Set cooldown time")
logger.info(" - get_cooldown() - Get current cooldown")
logger.info(" - is_cooling_down(user_id) - Check if user is in cooldown")

# Example usage in bot.py:
"""
from health import health_manager

async def process_txt_file(client, message, file_path, batch_name=None):
    try:
        # Extract valid URLs from the text file
        valid_urls = extract_urls_from_file(file_path)
        total_urls = len(valid_urls)
        
        if total_urls == 0:
            await message.reply_text("❌ No valid URLs found in the text file.")
            return
        
        # Send initial status message
        status_message = await message.reply_text(
            f"🔄 Processing {total_urls} URLs from text file...\n"
            f"ℹ️ Server Health Mode: ON (60s cooldown between files)\n"
            f"⏳ Please be patient and do not send another command."
        )
        
        # Process each URL
        success_count = 0
        failed_count = 0
        
        for i, url in enumerate(valid_urls, 1):
            # Update status with progress
            try:
                await status_message.edit_text(
                    f"🔄 Processing URL {i}/{total_urls}...\n"
                    f"✅ Success: {success_count} | ❌ Failed: {failed_count}\n"
                    f"📊 Progress: {(i-1)/total_urls*100:.1f}%"
                )
            except Exception:
                pass
                
            # Process the URL
            result = await process_url(client, message, url, batch_name, status_message)
            
            if result:
                success_count += 1
            else:
                failed_count += 1
            
            # Show cooldown timer between files (except for the last one)
            if i < total_urls:
                await health_manager.display_cooldown_message(
                    client, 
                    message.chat.id,
                    message.from_user.id,
                    i,
                    total_urls,
                    60,  # 60 second cooldown
                    status_message.id
                )
        
        # Final status update
        await status_message.edit_text(
            f"✅ Completed processing {total_urls} URLs!\n"
            f"• Success: {success_count}\n"
            f"• Failed: {failed_count}\n"
            f"• Total: {total_urls}"
        )
        
    except Exception as e:
        await message.reply_text(f"❌ An error occurred: {str(e)}")
""" 
