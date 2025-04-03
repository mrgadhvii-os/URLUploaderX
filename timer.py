import asyncio
import time
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Timer:
    def __init__(self):
        self.start_time = None
        self.pause_time = None
        self.is_paused = False
        self.total_pause_time = 0

    def start(self):
        """Start the timer"""
        self.start_time = time.time()
        self.total_pause_time = 0
        self.is_paused = False
        return self.start_time

    def pause(self):
        """Pause the timer"""
        if not self.is_paused:
            self.pause_time = time.time()
            self.is_paused = True

    def resume(self):
        """Resume the timer"""
        if self.is_paused:
            self.total_pause_time += time.time() - self.pause_time
            self.is_paused = False

    def get_elapsed(self):
        """Get elapsed time in seconds"""
        if not self.start_time:
            return 0
        
        current = time.time()
        if self.is_paused:
            return self.pause_time - self.start_time - self.total_pause_time
        return current - self.start_time - self.total_pause_time

    def reset(self):
        """Reset the timer"""
        self.start_time = None
        self.pause_time = None
        self.is_paused = False
        self.total_pause_time = 0

class CountdownTimer:
    def __init__(self, message=None):
        self.message = message
        self.is_running = False
        self.start_time = None
        self.duration = 0

    def format_time(self, seconds):
        """Format time in a modern way"""
        if seconds < 60:
            return f"{int(seconds)}s"
        minutes, seconds = divmod(int(seconds), 60)
        if minutes < 60:
            return f"{minutes}m {seconds}s"
        hours, minutes = divmod(minutes, 60)
        return f"{hours}h {minutes}m {seconds}s"

    def get_progress_bar(self, progress):
        """Create a modern progress bar"""
        bar_length = 10
        filled_length = int(bar_length * progress)
        bar = "█" * filled_length + "░" * (bar_length - filled_length)
        return bar

    async def start(self, duration, current_file=None, total_files=None, success_count=None, failed_count=None):
        """Start countdown timer with file processing stats"""
        self.is_running = True
        self.duration = duration
        self.start_time = time.time()

        try:
            while self.is_running and time.time() - self.start_time < duration:
                if not self.message:
                    await asyncio.sleep(1)
                    continue

                elapsed = time.time() - self.start_time
                remaining = max(0, duration - elapsed)
                progress = min(1.0, elapsed / duration)

                # Calculate estimated completion time
                if total_files and current_file:
                    remaining_files = total_files - current_file
                    eta_seconds = remaining_files * duration
                    completion_time = datetime.now() + timedelta(seconds=eta_seconds)
                    eta_str = completion_time.strftime("%I:%M %p")
                else:
                    eta_str = None

                # Create status message
                status_text = (
                    f"⏳ **Server Cooldown**\n\n"
                    f"{self.get_progress_bar(progress)} "
                    f"({int(progress * 100)}%)\n\n"
                )

                if current_file and total_files:
                    status_text += (
                        f"📊 **Progress:**\n"
                        f"• Files: {current_file}/{total_files}\n"
                        f"• Success: {success_count or 0}\n"
                        f"• Failed: {failed_count or 0}\n"
                        f"• Complete: {(current_file/total_files)*100:.1f}%\n\n"
                    )

                status_text += (
                    f"⏰ **Time Remaining:** {self.format_time(remaining)}\n"
                )

                if eta_str:
                    status_text += f"🎯 **Est. Completion:** {eta_str}\n"

                try:
                    await self.message.edit_text(
                        status_text,
                        parse_mode='markdown'
                    )
                except Exception as e:
                    logger.error(f"Error updating timer message: {e}")

                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error in countdown timer: {e}")
        finally:
            self.is_running = False

    def stop(self):
        """Stop the countdown timer"""
        self.is_running = False

async def delay_with_progress(message, delay_seconds, current_file=None, total_files=None, success_count=None, failed_count=None):
    """Utility function to create and start a countdown timer"""
    timer = CountdownTimer(message)
    await timer.start(
        delay_seconds,
        current_file=current_file,
        total_files=total_files,
        success_count=success_count,
        failed_count=failed_count
    )
    return timer

# Example usage in bot.py:
"""
from timer import delay_with_progress

# In your process_url_line function:
if current_file < total_files:  # Don't delay after last file
    await delay_with_progress(
        status_message,
        60,  # 60 seconds delay
        current_file=current_file,
        total_files=total_files,
        success_count=success_count,
        failed_count=failed_count
    )
""" 