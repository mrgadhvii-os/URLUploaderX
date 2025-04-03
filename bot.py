import asyncio
import os
import time
from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ForceReply,
    CallbackQuery,
)
from config import API_ID, API_HASH, BOT_TOKEN, AUTH_USERS, ADMIN_ID, OWNER_ID
from database import db
from downloader import Downloader
import logging
from pyrogram.enums import ParseMode
import traceback
import threading
import re
from datetime import datetime
import aiofiles
from PIL import Image
import logging.handlers
import glob
import json
import subprocess
from metadata_handler import ensure_video_metadata, format_duration
from txt_filter import process_text_file  # Add this import

# Create downloads directory if it doesn't exist
if not os.path.exists("downloads"):
    os.makedirs("downloads")

# Create logs directory if it doesn't exist
if not os.path.exists("logs"):
    os.makedirs("logs")

# Initialize bot
app = Client("url_uploader_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Set up rotating file handler for logs
log_file = "logs/bot.log"
file_handler = logging.handlers.RotatingFileHandler(
    log_file,
    maxBytes=1024 * 1024,  # 1MB
    backupCount=1
)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)

# Set up logger for bot with both file and stream handlers
logger = logging.getLogger("bot")
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(
    logging.Formatter("%(levelname)s - %(message)s")  # Simplified console format
)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# Set other loggers to WARNING level to reduce noise
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("pyrogram.client").setLevel(logging.WARNING)
logging.getLogger("pyrogram.connection.connection").setLevel(logging.WARNING)
logging.getLogger("pyrogram.dispatcher").setLevel(logging.WARNING)

# User states
USER_STATES = {}
# Lock for updating progress messages
update_locks = {}

# Add these global variables after USER_STATES
USER_THUMBNAILS = {}  # Store user thumbnails

# Add this new command handler
@app.on_message(filters.command("auth") & filters.private)
async def auth_user(client: Client, message: Message):
    try:
        # Check if command sender is admin or owner
        if message.from_user.id != ADMIN_ID and message.from_user.id != OWNER_ID:
            await message.reply_text(
                "❌ **Unauthorized!**\n\n"
                "Only the bot admin/owner can authorize users.",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        # Check if user ID is provided
        if len(message.command) != 2:
            await message.reply_text(
                "❌ **Invalid Format!**\n\n"
                "**Usage:** `/auth user_id`\n"
                "**Example:** `/auth 123456789`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        # Get the user ID to authorize
        try:
            user_id = int(message.command[1])
        except ValueError:
            await message.reply_text(
                "❌ **Invalid User ID!**\n\n"
                "Please provide a valid numeric user ID.",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        # Check if user is already authorized
        if user_id in AUTH_USERS:
            await message.reply_text(
                "⚠️ **Already Authorized!**\n\n"
                f"User ID `{user_id}` is already authorized to use the bot.",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        # Add user to authorized users
        AUTH_USERS.append(user_id)
        
        await message.reply_text(
            "✅ **User Authorized Successfully!**\n\n"
            f"User ID: `{user_id}`\n"
            "This user can now use the bot.",
            parse_mode=ParseMode.MARKDOWN
        )

        # Try to notify the authorized user
        try:
            await client.send_message(
                chat_id=user_id,
                text=(
                    "🎉 **Congratulations!**\n\n"
                    "You have been authorized to use the bot.\n"
                    "You can now start uploading files!\n\n"
                    "Use /start to begin."
                ),
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            logger.error(f"Failed to notify user {user_id}: {e}")
            await message.reply_text(
                "ℹ️ **Note:** Could not notify the user.\n"
                "They may need to start the bot first.",
                parse_mode=ParseMode.MARKDOWN
            )

    except Exception as e:
        logger.error(f"Error in auth command: {e}")
        logger.error(traceback.format_exc())
        await message.reply_text(
            "❌ **Error!**\n\n"
            f"An error occurred: `{str(e)}`",
            parse_mode=ParseMode.MARKDOWN
        )

# Add this command to list authorized users
@app.on_message(filters.command("listauth") & filters.private)
async def list_authorized(client: Client, message: Message):
    try:
        # Check if command sender is admin or owner
        if message.from_user.id != ADMIN_ID and message.from_user.id != OWNER_ID:
            await message.reply_text(
                "❌ **Unauthorized!**\n\n"
                "Only the bot admin/owner can view authorized users.",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        if not AUTH_USERS:
            await message.reply_text(
                "ℹ️ **No Authorized Users**\n\n"
                "Use /auth user_id to authorize users.",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        # Create list of authorized users
        auth_list = []
        for user_id in AUTH_USERS:
            try:
                user = await client.get_users(user_id)
                user_info = f"• `{user_id}` - {user.first_name}"
                if user.username:
                    user_info += f" (@{user.username})"
                auth_list.append(user_info)
            except Exception:
                auth_list.append(f"• `{user_id}` - Unknown User")

        await message.reply_text(
            "📋 **Authorized Users List:**\n\n" +
            "\n".join(auth_list) + "\n\n"
            "Use /auth user_id to authorize new users\n"
            "Use /unauth user_id to remove authorization",
            parse_mode=ParseMode.MARKDOWN
        )

    except Exception as e:
        logger.error(f"Error in listauth command: {e}")
        logger.error(traceback.format_exc())
        await message.reply_text(
            "❌ **Error!**\n\n"
            f"An error occurred: `{str(e)}`",
            parse_mode=ParseMode.MARKDOWN
        )

# Add this command to remove authorization
@app.on_message(filters.command("unauth") & filters.private)
async def unauth_user(client: Client, message: Message):
    try:
        # Check if command sender is admin or owner
        if message.from_user.id != ADMIN_ID and message.from_user.id != OWNER_ID:
            await message.reply_text(
                "❌ **Unauthorized!**\n\n"
                "Only the bot admin/owner can remove user authorization.",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        # Check if user ID is provided
        if len(message.command) != 2:
            await message.reply_text(
                "❌ **Invalid Format!**\n\n"
                "**Usage:** `/unauth user_id`\n"
                "**Example:** `/unauth 123456789`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        # Get the user ID to unauthorize
        try:
            user_id = int(message.command[1])
        except ValueError:
            await message.reply_text(
                "❌ **Invalid User ID!**\n\n"
                "Please provide a valid numeric user ID.",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        # Check if user is authorized
        if user_id not in AUTH_USERS:
            await message.reply_text(
                "⚠️ **Not Authorized!**\n\n"
                f"User ID `{user_id}` is not in the authorized users list.",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        # Remove user from authorized users
        AUTH_USERS.remove(user_id)
        
        # Update database if you're using one
        # If you have MongoDB setup, uncomment and modify these lines:
        # await db.authorized_users.update_one(
        #     {"type": "auth_users"},
        #     {"$pull": {"users": user_id}}
        # )

        await message.reply_text(
            "✅ **User Unauthorized Successfully!**\n\n"
            f"User ID: `{user_id}`\n"
            "This user can no longer use the bot.",
            parse_mode=ParseMode.MARKDOWN
        )

        # Try to notify the unauthorized user
        try:
            await client.send_message(
                chat_id=user_id,
                text=(
                    "⚠️ **Notice**\n\n"
                    "Your authorization to use the bot has been revoked.\n"
                    "Contact the bot admin if you think this is a mistake."
                ),
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            logger.error(f"Failed to notify user {user_id}: {e}")

    except Exception as e:
        logger.error(f"Error in unauth command: {e}")
        logger.error(traceback.format_exc())
        await message.reply_text(
            "❌ **Error!**\n\n"
            f"An error occurred: `{str(e)}`",
            parse_mode=ParseMode.MARKDOWN
        )

def format_size(size_bytes):
    """Format size in human-readable form"""
    if size_bytes is None or size_bytes == 0:
        return "0 B"

    # Convert to float and define units
    size_bytes = float(size_bytes)
    units = ["B", "KB", "MB", "GB", "TB"]

    # Determine appropriate unit
    i = 0
    while size_bytes >= 1024 and i < len(units) - 1:
        size_bytes /= 1024
        i += 1

    # Format with proper precision
    if i == 0:
        return f"{int(size_bytes)} {units[i]}"
    else:
        return f"{size_bytes:.2f} {units[i]}"


def create_progress_bar(progress):
    """Create a beautiful progress bar"""
    bar_length = 20
    filled_length = int(bar_length * progress / 100)
    bar = "█" * filled_length + "░" * (bar_length - filled_length)
    return bar


# Function to determine if file is a video
def is_video_file(file_path):
    """Check if the file is a video based on its extension"""
    video_extensions = [
        ".mp4",
        ".mkv",
        ".avi",
        ".mov",
        ".wmv",
        ".flv",
        ".webm",
        ".m4v",
        ".3gp",
        ".m3u8"  # Added support for HLS streaming
    ]
    ext = os.path.splitext(file_path)[1].lower()
    return ext in video_extensions


# Helper function to format ETA
def format_eta(seconds):
    if seconds is None or seconds <= 0:
        return "Almost done..."
    
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"


@app.on_message(filters.command("start"))
async def start_command(client: Client, message: Message):
    if message.from_user.id not in AUTH_USERS:
        await message.reply_text(
            "⚠️ You are not authorized to use this bot.\n"
            "Please contact the administrator for access.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "📞 Contact Admin", url="https://t.me/your_admin_username"
                        )
                    ]
                ]
            ),
        )
        return

    welcome_text = (
        "🌟 **Welcome to URL Uploader Bot!** 🌟\n\n"
        "This bot helps you download and upload files from various sources.\n\n"
        "**Features:**\n"
        "• Support for videos, PDFs, and images\n"
        "• Real-time download progress\n"
        "• Modern UI with progress bar\n"
        "• Decryption support for special videos\n"
        "• High-quality video uploads with thumbnails\n"
        "• Batch processing from text files\n\n"
        "Choose an option to begin:"
    )

    USER_STATES[message.from_user.id] = {"state": "choosing_mode"}
    await message.reply_text(
        welcome_text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📝 Single URL Upload", callback_data="mode_single")],
            [InlineKeyboardButton("📁 Upload from Text File", callback_data="mode_txt")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
            [InlineKeyboardButton("ℹ️ Help", callback_data="help")]
        ])
    )


@app.on_message(filters.command("stop"))
async def stop_command(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id in USER_STATES:
        # Cancel any active downloads
        if USER_STATES[user_id].get("canceled") != True:
            USER_STATES[user_id]["canceled"] = True

        del USER_STATES[user_id]
        await message.reply_text(
            "👋 Thank you for using URL Uploader Bot!\n\n"
            "Send /start to begin a new session.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔄 Start New Session", callback_data="start")]]
            ),
        )
    else:
        await message.reply_text(
            "❌ No active session found.\n\n" "Send /start to begin a new session.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔄 Start New Session", callback_data="start")]]
            ),
        )


@app.on_message(filters.text & filters.private & ~filters.command(["start", "stop"]))
async def handle_messages(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in AUTH_USERS:
        return

    if user_id not in USER_STATES:
        await start_command(client, message)
        return

    state = USER_STATES[user_id].get("state")

    if state == "waiting_username":
        username = message.text
        if not username.startswith("@"):
            await message.reply_text(
                "⚠️ Invalid username format!\n"
                "Please provide a valid username starting with @",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]]
                ),
            )
            return

        USER_STATES[user_id].update(
            {"username": username, "state": "waiting_batch_name"}
        )

        await message.reply_text(
            "📝 Please provide a batch name for your files:\n\n"
            "Example: `URL Uploader 2024`",
            reply_markup=ForceReply(selective=True),
        )

    elif state == "waiting_batch_name":
        batch_name = message.text.strip()
        USER_STATES[user_id].update(
            {"batch_name": batch_name, "state": "waiting_file_url"}
        )

        await message.reply_text(
            "📝 Please send the file details in the format:\n\n"
            "`Filename : URL`\n\n"
            "Example:\n"
            "`My Video : https://example.com/video.mp4`\n"
            "`Encrypted Video : https://example.com/video.mkv*12345`\n\n"
            "Use /stop to end the session at any time.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=ForceReply(selective=True),
        )

    elif state == "waiting_file_url":
        if ":" not in message.text:
            await message.reply_text(
                "⚠️ Invalid format!\n\n"
                "Please use the format:\n"
                "`Filename : URL`\n"
                "For encrypted videos: `Filename : URL.mkv*key`\n\n"
                "Examples:\n"
                "`My Video : https://example.com/video.mp4`\n"
                "`Encrypted Video : https://example.com/video.mkv*12345`",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=ForceReply(selective=True),
            )
            return

        filename, url = [x.strip() for x in message.text.split(":", 1)]
        if not filename or not url:
            await message.reply_text(
                "⚠️ Invalid format!\n\n" "Please provide both filename and URL.",
                reply_markup=ForceReply(selective=True),
            )
            return

        # Check if it's an encrypted video URL
        is_encrypted = "*" in url and any(
            ext in url.lower()
            for ext in [".mkv", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".webm"]
        )

        # Store current download info in case user wants to cancel
        USER_STATES[user_id]["current_task"] = {
            "filename": filename,
            "url": url,
            "is_encrypted": is_encrypted,
            "status_message": None,
            "last_update_time": 0,
            "progress": 0,
            "downloaded_size": 0,
            "total_size": 0,
            "speed": 0,
            "eta": 0
        }

        # Set canceled flag to False for new download
        USER_STATES[user_id]["canceled"] = False
        
        # Create lock for this user if it doesn't exist
        if user_id not in update_locks:
            update_locks[user_id] = threading.Lock()

        # Initial status message
        status_message = await message.reply_text(
            f"{'🔐 Dᴇᴄʀʏᴘᴛɪɴɢ & ' if is_encrypted else ''}Dᴏᴡɴʟᴏᴀᴅ Sᴛᴀʀᴛᴇᴅ....\n\n"
            f"{create_progress_bar(0)}\n\n"
            "╭━━━━❰ᴘʀᴏɢʀᴇss ʙᴀʀ❱━➣\n"
            "┣⪼ 🗃️ Sɪᴢᴇ: Waiting... \n"
            "┣⪼ ⏳️ Dᴏɴᴇ : 0%\n"
            "┣⪼ 🚀 Sᴩᴇᴇᴅ: Calculating...\n"
            "┣⪼ ⏰️ Eᴛᴀ: Calculating...\n"
            "╰━━━━━━━━━━━━━━━➣",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_download")]]
            ),
        )

        # Save status message for potential cancellation and updates
        USER_STATES[user_id]["current_task"]["status_message"] = status_message
        USER_STATES[user_id]["current_task"]["message_id"] = status_message.id
        USER_STATES[user_id]["current_task"]["chat_id"] = status_message.chat.id
        
        # Progress callback - updates the status message
        async def progress_callback(
            progress, speed, total_size, downloaded_size, eta, filename=""
        ):
            try:
                # Check if user has canceled
                if user_id not in USER_STATES or USER_STATES[user_id].get(
                    "canceled", False
                ):
                    return

                # Get current task info
                current_task = USER_STATES[user_id].get("current_task", {})
                current_message = current_task.get("status_message")

                # If no message to update, exit
                if not current_message:
                    logger.warning(f"No status message found for user {user_id}")
                    return
                
                # Use lock to prevent multiple concurrent updates
                with update_locks[user_id]:
                    # Only update if enough time has passed since last update (rate limiting)
                    now = time.time()
                    last_update = current_task.get("last_update_time", 0)
                    if now - last_update < 0.5:  # 0.5 seconds minimum between updates
                        return
                
                    # Update progress info
                    current_task.update({
                        "progress": progress,
                        "downloaded_size": downloaded_size,
                        "total_size": total_size,
                        "speed": speed,
                        "eta": eta
                    })
                
                    # Create progress text
                    progress_bar = create_progress_bar(progress)
                    status_text = (
                        f"{'🔐 Dᴇᴄʀʏᴘᴛɪɴɢ & ' if is_encrypted else ''}Dᴏᴡɴʟᴏᴀᴅɪɴɢ....\n\n"
                        f"{progress_bar}\n\n"
                        "╭━━━━❰ᴘʀᴏɢʀᴇss ʙᴀʀ❱━➣\n"
                        f"┣⪼ 🗃️ Sɪᴢᴇ: {format_size(downloaded_size)} / {format_size(total_size)}\n"
                        f"┣⪼ ⏳️ Dᴏɴᴇ : {progress:.1f}%\n"
                        f"┣⪼ 🚀 Sᴩᴇᴇᴅ: {format_size(speed)}/s\n"
                        f"┣⪼ ⏰️ Eᴛᴀ: {format_eta(eta)}\n"
                        "╰━━━━━━━━━━━━━━━➣"
                    )
    
                    # Update the message with new progress
                    try:
                        await current_message.edit_text(
                            status_text,
                            reply_markup=InlineKeyboardMarkup(
                                [
                                    [
                                        InlineKeyboardButton(
                                            "❌ Cancel", callback_data="cancel_download"
                                        )
                                    ]
                                ]
                            ),
                        )
                        # Log successful update
                        logger.info(f"Updated progress for user {user_id}: {progress:.1f}%")
                        # Store the last update time
                        current_task["last_update_time"] = now
                    except Exception as e:
                        logger.error(f"Failed to update progress message: {e}")

            except Exception as e:
                # Log the full exception with traceback
                logger.error(f"Progress callback error: {e}")
                logger.error(traceback.format_exc())

        try:
            # Create and start downloader
            downloader = Downloader(url, filename, progress_callback)
            success, result, video_info = await downloader.download()

            # Check if user has canceled during download
            if user_id not in USER_STATES or USER_STATES[user_id].get(
                "canceled", False
            ):
                if result and os.path.exists(result):
                    os.remove(result)
                if (
                    video_info
                    and video_info.thumbnail
                    and os.path.exists(video_info.thumbnail)
                ):
                    os.remove(video_info.thumbnail)
                return

            if not success:
                await status_message.edit_text(
                    f"❌ Download failed!\n\n"
                    f"Error: {result}\n\n"
                    f"Please try again or contact support if the problem persists.",
                    reply_markup=InlineKeyboardMarkup(
                        [
                            [
                                InlineKeyboardButton(
                                    "🔄 Try Again", callback_data="continue"
                                )
                            ]
                        ]
                    ),
                )
                return

            await status_message.edit_text(
                "📤 Uploading to Telegram...\n\n"
                "Please wait while we upload your file."
            )

            # Different caption formats for different file types
            caption = (
                "➖➖➖➖➖➖➖➖➖➖\n"
                "📂 **File Details**\n"
                "➖➖➖➖➖➖➖➖➖➖\n"
                f"📝 **File Name:** `{filename}`\n"
                f"👤 **Downloaded By:** {USER_STATES[user_id]['username']}\n"
                f"🎯 **Batch:** `{USER_STATES[user_id]['batch_name']}`\n"
                f"⚡ **Status:** ✅ __Successfully Processed__\n"
                "\n"
                "🔗 __Stay Connected:__ [@MrGadhvii](https://t.me/MrGadhvii)\n"
                "➖➖➖➖➖➖➖➖➖➖"
            )

            # Last upload update time
            last_upload_update_time = time.time()
            update_interval = 1  # seconds between updates

            # Progress callback for upload with rate limiting
            async def upload_progress(current, total):
                nonlocal last_upload_update_time
                current_time = time.time()

                # Throttle updates to avoid Telegram's rate limits
                if (current_time - last_upload_update_time) < update_interval:
                    return

                last_upload_update_time = current_time

                try:
                    # Check if user has canceled
                    if user_id not in USER_STATES or USER_STATES[user_id].get(
                        "canceled", False
                    ):
                        return

                    progress = (current / total) * 100
                    progress_bar = create_progress_bar(progress)
                    status_text = (
                        "📤 Uᴘʟᴏᴀᴅɪɴɢ....\n\n"
                        f"{progress_bar}\n\n"
                        "╭━━━━❰ᴘʀᴏɢʀᴇss ʙᴀʀ❱━➣\n"
                        f"┣⪼ 🗃️ Sɪᴢᴇ: {format_size(current)} / {format_size(total)}\n"
                        f"┣⪼ ⏳️ Dᴏɴᴇ : {progress:.1f}%\n"
                        "╰━━━━━━━━━━━━━━━➣"
                    )
                    await status_message.edit_text(status_text)
                    logger.info(f"Upload progress for user {user_id}: {progress:.1f}%")
                except Exception as e:
                    logger.error(f"Upload progress error: {e}")

            # Get custom thumbnail if exists, otherwise use video thumbnail
            thumbnail_path = USER_THUMBNAILS.get(user_id)
            if not thumbnail_path and video_info and video_info.thumbnail:
                thumbnail_path = video_info.thumbnail

            # Send as video if it's a video file, otherwise as document
            if is_video_file(result):
                try:
                    # Process video metadata
                    logger.info("Processing video metadata...")
                    metadata = await ensure_video_metadata(result)
                    
                    if not metadata:
                        raise ValueError("Failed to process video metadata")
                    
                    # Get custom thumbnail if exists, otherwise use generated one
                    thumbnail_path = USER_THUMBNAILS.get(user_id) or metadata['thumbnail']
                    
                    # Enhanced caption with duration
                    caption = (
                        "➖➖➖➖➖➖➖➖➖➖\n"
                        "📂 **Video Details**\n"
                        "➖➖➖➖➖➖➖➖➖➖\n"
                        f"📝 **File Name:** `{filename}`\n"
                        f"⏱️ **Duration:** `{metadata['duration_text']}`\n"
                        f"📊 **Quality:** `{metadata['height']}p`\n"
                        f"👤 **Downloaded By:** {USER_STATES[user_id]['username']}\n"
                        f"🎯 **Batch:** `{USER_STATES[user_id]['batch_name']}`\n"
                        f"⚡ **Status:** ✅ __Successfully Processed__\n"
                        "\n"
                        "🔗 __Stay Connected:__ [@MrGadhvii](https://t.me/MrGadhvii)\n"
                        "➖➖➖➖➖➖➖➖➖➖"
                    )
                    
                    try:
                        # First attempt with all parameters
                        logger.info("Attempting to send video with full parameters...")
                        await message.reply_video(
                            video=result,
                            caption=caption,
                            parse_mode=ParseMode.MARKDOWN,
                            duration=metadata['duration'],
                            width=metadata['width'],
                            height=metadata['height'],
                            thumb=thumbnail_path,
                            supports_streaming=True,
                            progress=upload_progress
                        )
                        logger.info(f"Video sent successfully: {filename}")
                    except Exception as e:
                        logger.error(f"First attempt failed: {str(e)}")
                        try:
                            # Second attempt with minimal parameters
                            logger.info("Attempting to send video with minimal parameters...")
                            await message.reply_video(
                                video=result,
                                caption=caption,
                                parse_mode=ParseMode.MARKDOWN,
                                duration=metadata['duration'],
                                thumb=thumbnail_path,
                                supports_streaming=True
                            )
                            logger.info(f"Video sent successfully with minimal parameters: {filename}")
                        except Exception as e:
                            logger.error(f"Second attempt failed: {str(e)}")
                            try:
                                # Final attempt with bare minimum
                                logger.info("Final attempt with bare minimum parameters...")
                                await message.reply_video(
                                    video=result,
                                    caption=caption,
                                    supports_streaming=True
                                )
                                logger.info(f"Video sent with bare minimum parameters: {filename}")
                            except Exception as e:
                                logger.error(f"All attempts failed: {str(e)}")
                                raise e
                except Exception as e:
                    logger.error(f"Error processing video: {e}")
                    logger.error(traceback.format_exc())
                    raise e
            else:
                logger.info(f"Sending document: {filename}")
                
                # Get custom thumbnail for PDFs
                thumbnail_path = None
                if filename.lower().endswith('.pdf'):
                    thumbnail_path = USER_THUMBNAILS.get(user_id)
                    if thumbnail_path:
                        logger.info(f"Using custom thumbnail for PDF: {thumbnail_path}")
                
                # FORCE PDF EXTENSION FOR PDF FILES - Final check before sending
                if ('pdf' in url.lower() or '.pdf*' in url.lower()):
                    if not filename.lower().endswith('.pdf'):
                        old_filename = filename
                        filename = f"{os.path.splitext(filename)[0]}.pdf"
                        logger.info(f"FORCED PDF extension before sending: {old_filename} -> {filename}")
                
                try:
                    # Simplified caption for PDFs
                    if filename.lower().endswith('.pdf'):
                        caption = (
                            f"📝 **{filename}**\n"
                            f"👤 **By:** {USER_STATES[user_id]['username']}\n"
                            f"🎯 **Batch:** `{USER_STATES[user_id]['batch_name']}`\n"
                            "🔗 [@MrGadhvii](https://t.me/MrGadhvii)"
                        )
                    else:
                        caption = (
                            f"📝 **{filename}**\n"
                            f"👤 **By:** {USER_STATES[user_id]['username']}\n"
                            f"🎯 **Batch:** `{USER_STATES[user_id]['batch_name']}`"
                        )
                    
                    await message.reply_document(
                        document=result,
                        caption=caption,
                        parse_mode=ParseMode.MARKDOWN,
                        thumb=thumbnail_path,
                        progress=upload_progress,
                        file_name=filename
                    )
                    logger.info(f"Document sent successfully: {filename}")
                    
                    # Clean up files after successful upload
                    clean_all_files()
                    
                except Exception as e:
                    logger.error(f"Error sending document: {e}")
                    try:
                        # If sending fails, try once more with explicit PDF extension
                        if ('pdf' in url.lower() or '.pdf*' in url.lower()) and not filename.lower().endswith('.pdf'):
                            filename = f"{filename}.pdf"
                            logger.info(f"Emergency PDF fix - Adding extension: {filename}")
                        
                        await message.reply_document(
                            document=result,
                            caption=caption,
                            parse_mode=ParseMode.MARKDOWN,
                            thumb=thumbnail_path,
                            progress=upload_progress,
                            file_name=filename
                        )
                        logger.info(f"Document sent successfully on retry: {filename}")
                        
                        # Clean up files after successful upload
                        clean_all_files()
                        
                    except Exception as e:
                        logger.error(f"Error sending document on retry: {e}")
                        raise e

            # Clean up files
            if os.path.exists(result):
                os.remove(result)
                logger.info(f"Removed downloaded file: {result}")
            if thumbnail_path and os.path.exists(thumbnail_path) and thumbnail_path != USER_THUMBNAILS.get(user_id):
                # Only remove auto-generated thumbnails, not user's custom thumbnail
                os.remove(thumbnail_path)
                logger.info(f"Removed thumbnail: {thumbnail_path}")

            # Clean any JSON files
            clean_downloads_dir()
            
            # Clean logs after successful upload
            clean_logs()

            # Delete status message
            try:
                await status_message.delete()
            except Exception as e:
                logger.error(f"Failed to delete status message: {e}")

            await message.reply_text(
                "✅ File uploaded successfully!\n\n"
                "Would you like to download another file?",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton("✅ Yes", callback_data="continue"),
                            InlineKeyboardButton("❌ No", callback_data="stop"),
                        ]
                    ]
                ),
            )
        except Exception as e:
            logger.error(f"Download/upload error: {e}")
            logger.error(traceback.format_exc())
            await status_message.edit_text(
                f"❌ An error occurred!\n\n"
                f"Error: {str(e)}\n\n"
                f"Please try again or contact support if the problem persists.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔄 Try Again", callback_data="continue")]]
                ),
            )


@app.on_callback_query()
async def answer_callback(client: Client, callback_query: CallbackQuery):
    data = callback_query.data
    user_id = callback_query.from_user.id
    message = callback_query.message

    # Check authorization for protected actions
    protected_actions = ["mode_single", "mode_txt", "continue", "cancel_download", "cancel_batch"]
    if data in protected_actions and user_id not in AUTH_USERS:
        await callback_query.answer("⚠️ You are not authorized to use this bot.", show_alert=True)
        return

    if data == "start":
        if user_id in AUTH_USERS:
            await start_command(client, callback_query.message)
        else:
            await message.edit_text(
                "⚠️ You are not authorized to use this bot.\n"
                "Please contact the administrator for access.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("👨‍💻 Contact Admin", url="https://t.me/MrGadhvii")],
                    [InlineKeyboardButton("📢 Updates Channel", url="https://t.me/MrGadhvii")],
                    [InlineKeyboardButton("ℹ️ Help", callback_data="help")]
                ])
            )

    elif data == "help":
        await help_command(client, callback_query.message)

    elif data == "mode_single":
        if user_id not in USER_STATES:
            USER_STATES[user_id] = {}
        
        USER_STATES[user_id]["state"] = "waiting_username"
        await message.edit_text(
            "Please provide your @username to continue.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]]
            )
        )

    elif data == "mode_txt":
        if user_id not in USER_STATES:
            USER_STATES[user_id] = {}
            
        USER_STATES[user_id].update({
            "state": "waiting_txt",
            "username": callback_query.from_user.username or "Anonymous",
            "batch_name": "Batch " + datetime.now().strftime("%Y-%m-%d %H:%M")
        })
        
        await message.edit_text(
            "📤 **Send your text file containing URLs**\n\n"
            "File should contain URLs in the format:\n"
            "`filename : url`\n\n"
            "**Example content:**\n"
            "```\n"
            "video1.mp4 : https://example.com/video1.mp4\n"
            "doc1.pdf : https://example.com/document.pdf\n"
            "video2.mkv : https://example.com/encrypted.mkv*key\n"
            "```\n\n"
            "• One URL per line\n"
            "• Supports PDF and video files\n"
            "• Automatically skips ZIP and YouTube URLs\n"
            "• Supports encrypted video URLs\n\n"
            "Send your .txt file now, then reply to it with /txt command.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
                [InlineKeyboardButton("🔙 Back to Menu", callback_data="start")]
            ])
        )

    elif data == "cancel":
        if user_id in USER_STATES:
            del USER_STATES[user_id]
        
        await message.edit_text(
            "❌ Operation cancelled.\n\n" "Send /start to begin a new session.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔄 Start New Session", callback_data="start")]]
            ),
        )

    elif data == "cancel_download":
        # Mark the download as canceled
        if user_id in USER_STATES:
            USER_STATES[user_id]["canceled"] = True
            logger.info(f"User {user_id} canceled download")
            
            await message.edit_text(
                "❌ Download cancelled.\n\n" "Send /start to begin a new session.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔄 Start New Session", callback_data="start")]]
                ),
            )

    elif data == "continue":
        if user_id in USER_STATES:
            USER_STATES[user_id].update({"state": "waiting_file_url"})

            await message.edit_text(
                "📝 Please send the file details in the format:\n\n"
                "`Filename : URL`\n\n"
                "Example:\n"
                "`My Video : https://example.com/video.mp4`\n"
                "`Encrypted Video : https://example.com/video.mkv*12345`\n\n"
                "Use /stop to end the session at any time.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=None,
            )

    elif data == "stop":
        if user_id in USER_STATES:
            del USER_STATES[user_id]

        await message.edit_text(
            "👋 Thank you for using URL Uploader Bot!\n\n"
            "Send /start to begin a new session.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔄 Start New Session", callback_data="start")]]
            ),
        )

    await callback_query.answer()


def clean_downloads_dir():
    """Clean the downloads directory by removing .json files"""
    try:
        for file in os.listdir("downloads"):
            if file.endswith(".json"):
                os.remove(os.path.join("downloads", file))
                logger.info(f"Removed JSON file: {file}")
    except Exception as e:
        logger.error(f"Error cleaning downloads directory: {e}")


def parse_line(line):
    """Parse a line to extract filename and URL regardless of format"""
    try:
        # Handle format like "TEST:https://..."
        if ':http' in line:
            parts = line.split(':http', 1)
            if len(parts) == 2:
                filename = parts[0].strip()
                url = 'http' + parts[1].strip()
                logger.info(f"Parsed line - Filename: {filename}, URL: {url}")
                
                # Handle encrypted PDF URLs (pdf*key format)
                if '.pdf*' in url.lower():
                    logger.info("Encrypted PDF URL detected")
                    if not filename.lower().endswith('.pdf'):
                        filename = f"{filename}.pdf"
                        logger.info(f"Added .pdf extension for encrypted PDF: {filename}")
                # Regular PDF handling
                elif url.lower().endswith('.pdf'):
                    if not filename.lower().endswith('.pdf'):
                        filename = f"{filename}.pdf"
                        logger.info(f"Added .pdf extension for PDF: {filename}")
                
                return filename, url
                
        # Find the first occurrence of http:// or https://
        url_start = line.find('http://')
        if url_start == -1:
            url_start = line.find('https://')
        
        if url_start == -1:
            logger.warning(f"No URL found in line: {line}")
            return None, None
            
        # Everything before the URL is the filename, everything after is the URL
        filename = line[:url_start].strip()
        url = line[url_start:].strip()
        
        # Clean up filename (remove trailing colons and spaces)
        filename = filename.rstrip(':').strip()
        
        # If filename is empty, use a default name
        if not filename:
            filename = "File_" + datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Handle encrypted PDF URLs
        if '.pdf*' in url.lower():
            logger.info("Encrypted PDF URL detected")
            if not filename.lower().endswith('.pdf'):
                filename = f"{filename}.pdf"
                logger.info(f"Added .pdf extension for encrypted PDF: {filename}")
        # Regular PDF handling
        elif url.lower().endswith('.pdf'):
            if not filename.lower().endswith('.pdf'):
                filename = f"{filename}.pdf"
                logger.info(f"Added .pdf extension for PDF: {filename}")
            
        logger.info(f"Parsed line - Filename: {filename}, URL: {url}")
        return filename, url
    except Exception as e:
        logger.error(f"Error parsing line: {e}")
        return None, None


async def process_url_line(client: Client, message: Message, line: str, user_id: int):
    try:
        if not line.strip():
            return False
        
        filename, url = parse_line(line)
        if not filename or not url:
            logger.info(f"Invalid line format: {line}")
            return False
        
        # Skip zip files
        if url.lower().endswith('.zip'):
            logger.info(f"Skipping zip file: {url}")
            return False
            
        # Skip YouTube URLs
        if "youtube.com" in url.lower() or "youtu.be" in url.lower():
            logger.info(f"Skipping YouTube URL: {url}")
            return False
            
        # Check for video files and PDFs (including encrypted ones)
        base_url = url.split('*')[0].lower()  # Get URL without encryption key
        is_video = any(base_url.endswith(ext) for ext in ['.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm'])
        is_pdf = base_url.endswith('.pdf') or ('pdf' in base_url and '*' in url)  # Handle both normal and encrypted PDFs
        
        if not (is_video or is_pdf):
            logger.info(f"Skipping unsupported file type: {url}")
            return False

        # Store current download info
        USER_STATES[user_id]["current_task"] = {
            "filename": filename,
            "url": url,
            "is_encrypted": '*' in url,
            "status_message": None,
            "last_update_time": 0,
            "progress": 0,
            "downloaded_size": 0,
            "total_size": 0,
            "speed": 0,
            "eta": 0
        }
        
        # Send initial status message with encryption indicator
        status_message = await message.reply_text(
            f"{'🔐 Dᴇᴄʀʏᴘᴛɪɴɢ & ' if '*' in url else ''}Dᴏᴡɴʟᴏᴀᴅ Sᴛᴀʀᴛᴇᴅ....\n\n"
            f"{create_progress_bar(0)}\n\n"
            "╭━━━━❰ᴘʀᴏɢʀᴇss ʙᴀʀ❱━➣\n"
            "┣⪼ 🗃️ Sɪᴢᴇ: Waiting... \n"
            "┣⪼ ⏳️ Dᴏɴᴇ : 0%\n"
            "┣⪼ 🚀 Sᴩᴇᴇᴅ: Calculating...\n"
            "┣⪼ ⏰️ Eᴛᴀ: Calculating...\n"
            "╰━━━━━━━━━━━━━━━➣",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_download")]]
            )
        )
        
        USER_STATES[user_id]["current_task"]["status_message"] = status_message
        
        # Progress callback
        async def progress_callback(progress, speed, total_size, downloaded_size, eta, filename=""):
            try:
                if user_id not in USER_STATES or USER_STATES[user_id].get("canceled", False):
                    return
                
                current_task = USER_STATES[user_id].get("current_task", {})
                if not current_task:
                    return
                    
                # Update progress info
                current_task.update({
                    "progress": progress,
                    "downloaded_size": downloaded_size,
                    "total_size": total_size,
                    "speed": speed,
                    "eta": eta
                })
                
                # Only update message if enough time has passed
                now = time.time()
                last_update = current_task.get("last_update_time", 0)
                if now - last_update < 0.5:
                    return
                
                current_message = current_task.get("status_message")
                if not current_message:
                    return
                
                progress_bar = create_progress_bar(progress)
                status_text = (
                    f"{'🔐 Dᴇᴄʀʏᴘᴛɪɴɢ & ' if '*' in url else ''}Dᴏᴡɴʟᴏᴀᴅɪɴɢ....\n\n"
                    f"{progress_bar}\n\n"
                    "╭━━━━❰ᴘʀᴏɢʀᴇss ʙᴀʀ❱━➣\n"
                    f"┣⪼ 🗃️ Sɪᴢᴇ: {format_size(downloaded_size)} / {format_size(total_size)}\n"
                    f"┣⪼ ⏳️ Dᴏɴᴇ : {progress:.1f}%\n"
                    f"┣⪼ 🚀 Sᴩᴇᴇᴅ: {format_size(speed)}/s\n"
                    f"┣⪼ ⏰️ Eᴛᴀ: {format_eta(eta)}\n"
                    "╰━━━━━━━━━━━━━━━➣"
                )
                
                try:
                    await current_message.edit_text(
                        status_text,
                        reply_markup=InlineKeyboardMarkup(
                            [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_download")]]
                        )
                    )
                    current_task["last_update_time"] = now
                    logger.info(f"Progress update: {progress:.1f}% at {format_size(speed)}/s")
                except Exception as e:
                    logger.error(f"Failed to update progress message: {e}")
                    
            except Exception as e:
                logger.error(f"Progress callback error: {e}")
                logger.error(traceback.format_exc())
        
        # Create and start downloader with proper handling for encrypted files
        downloader = Downloader(url, filename, progress_callback)
        success, result, video_info = await downloader.download()
        
        if not success:
            await status_message.edit_text(f"❌ Download failed!\n\nError: {result}")
            return False
        
        # For encrypted files, add a small delay to ensure decryption is complete
        if '*' in url:
            logger.info("Waiting for encrypted file decryption...")
            await asyncio.sleep(3)
        
        await status_message.edit_text(
            "📤 Uploading to Telegram...\n\n"
            f"{create_progress_bar(0)}\n\n"
            "╭━━━━❰ᴘʀᴏɢʀᴇss ʙᴀʀ❱━➣\n"
            "┣⪼ 🗃️ Sɪᴢᴇ: Calculating...\n"
            "┣⪼ ⏳️ Dᴏɴᴇ : 0%\n"
            "╰━━━━━━━━━━━━━━━➣"
        )
        
        # Upload progress callback
        last_upload_update_time = time.time()
        update_interval = 1
        
        async def upload_progress(current, total):
            nonlocal last_upload_update_time
            current_time = time.time()
            
            if (current_time - last_upload_update_time) < update_interval:
                return
                
            last_upload_update_time = current_time
            
            try:
                progress = (current / total) * 100
                progress_bar = create_progress_bar(progress)
                status_text = (
                    "📤 Uᴘʟᴏᴀᴅɪɴɢ....\n\n"
                    f"{progress_bar}\n\n"
                    "╭━━━━❰ᴘʀᴏɢʀᴇss ʙᴀʀ❱━➣\n"
                    f"┣⪼ 🗃️ Sɪᴢᴇ: {format_size(current)} / {format_size(total)}\n"
                    f"┣⪼ ⏳️ Dᴏɴᴇ : {progress:.1f}%\n"
                    "╰━━━━━━━━━━━━━━━➣"
                )
                await status_message.edit_text(status_text)
                logger.info(f"Upload progress: {progress:.1f}%")
            except Exception as e:
                logger.error(f"Upload progress error: {e}")
        
        try:
            # Get custom thumbnail if exists
            thumbnail_path = USER_THUMBNAILS.get(user_id)

            # Send as video if it's a video file, otherwise as document
            if is_video_file(result):
                try:
                    # Process video metadata
                    logger.info("Processing video metadata...")
                    metadata = await ensure_video_metadata(result)
                    
                    if not metadata:
                        raise ValueError("Failed to process video metadata")
                    
                    # Get custom thumbnail if exists, otherwise use generated one
                    thumbnail_path = USER_THUMBNAILS.get(user_id) or metadata['thumbnail']
                    
                    # Enhanced caption with duration
                    caption = (
                        "➖➖➖➖➖➖➖➖➖➖\n"
                        "📂 **Video Details**\n"
                        "➖➖➖➖➖➖➖➖➖➖\n"
                        f"📝 **File Name:** `{filename}`\n"
                        f"⏱️ **Duration:** `{metadata['duration_text']}`\n"
                        f"📊 **Quality:** `{metadata['height']}p`\n"
                        f"👤 **Downloaded By:** {USER_STATES[user_id]['username']}\n"
                        f"🎯 **Batch:** `{USER_STATES[user_id]['batch_name']}`\n"
                        f"⚡ **Status:** ✅ __Successfully Processed__\n"
                        "\n"
                        "🔗 __Stay Connected:__ [@MrGadhvii](https://t.me/MrGadhvii)\n"
                        "➖➖➖➖➖➖➖➖➖➖"
                    )
                    
                    try:
                        # First attempt with all parameters
                        logger.info("Attempting to send video with full parameters...")
                        await message.reply_video(
                            video=result,
                            caption=caption,
                            parse_mode=ParseMode.MARKDOWN,
                            duration=metadata['duration'],
                            width=metadata['width'],
                            height=metadata['height'],
                            thumb=thumbnail_path,
                            supports_streaming=True,
                            progress=upload_progress
                        )
                        logger.info(f"Video sent successfully: {filename}")
                    except Exception as e:
                        logger.error(f"First attempt failed: {str(e)}")
                        try:
                            # Second attempt with minimal parameters
                            logger.info("Attempting to send video with minimal parameters...")
                            await message.reply_video(
                                video=result,
                                caption=caption,
                                parse_mode=ParseMode.MARKDOWN,
                                duration=metadata['duration'],
                                thumb=thumbnail_path,
                                supports_streaming=True
                            )
                            logger.info(f"Video sent successfully with minimal parameters: {filename}")
                        except Exception as e:
                            logger.error(f"Second attempt failed: {str(e)}")
                            try:
                                # Final attempt with bare minimum
                                logger.info("Final attempt with bare minimum parameters...")
                                await message.reply_video(
                                    video=result,
                                    caption=caption,
                                    supports_streaming=True
                                )
                                logger.info(f"Video sent with bare minimum parameters: {filename}")
                            except Exception as e:
                                logger.error(f"All attempts failed: {str(e)}")
                                raise e
                except Exception as e:
                    logger.error(f"Error processing video: {e}")
                    logger.error(traceback.format_exc())
                    raise e
            else:
                logger.info(f"Sending document: {filename}")
                
                # Get custom thumbnail for PDFs
                thumbnail_path = None
                if filename.lower().endswith('.pdf'):
                    thumbnail_path = USER_THUMBNAILS.get(user_id)
                    if thumbnail_path:
                        logger.info(f"Using custom thumbnail for PDF: {thumbnail_path}")
                
                # Ensure PDF extension for encrypted PDFs
                if ('pdf' in url.lower() or '.pdf*' in url.lower()):
                    if not filename.lower().endswith('.pdf'):
                        old_filename = filename
                        filename = f"{os.path.splitext(filename)[0]}.pdf"
                        logger.info(f"Added .pdf extension: {old_filename} -> {filename}")
                
                try:
                    # Simplified caption for PDFs
                    caption = (
                        f"📝 **{filename}**\n"
                        f"👤 **By:** {USER_STATES[user_id]['username']}\n"
                        f"🎯 **Batch:** `{USER_STATES[user_id]['batch_name']}`\n"
                        "🔗 [@MrGadhvii](https://t.me/MrGadhvii)"
                    )
                    
                    await message.reply_document(
                        document=result,
                        caption=caption,
                        parse_mode=ParseMode.MARKDOWN,
                        thumb=thumbnail_path,
                        progress=upload_progress,
                        file_name=filename
                    )
                    logger.info(f"Document sent successfully: {filename}")
                    
                    # Clean up files after successful upload
                    clean_all_files()
                    
                except Exception as e:
                    logger.error(f"Error sending document: {e}")
                    raise e
            
            # Clean up files
            if os.path.exists(result):
                os.remove(result)
                logger.info(f"Removed downloaded file: {result}")
            if thumbnail_path and os.path.exists(thumbnail_path) and thumbnail_path != USER_THUMBNAILS.get(user_id):
                os.remove(thumbnail_path)
                logger.info(f"Removed thumbnail: {thumbnail_path}")
            
            # Clean any JSON files
            clean_downloads_dir()
            
            # Clean logs after successful upload
            clean_logs()
            
            # Delete status message
            try:
                await status_message.delete()
            except Exception as e:
                logger.error(f"Failed to delete status message: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"Upload error: {e}")
            logger.error(traceback.format_exc())
            await status_message.edit_text(f"❌ Upload failed!\n\nError: {str(e)}")
            return False
            
    except Exception as e:
        logger.error(f"Error processing URL line: {e}")
        logger.error(traceback.format_exc())
        return False


@app.on_message(filters.command("txt"))
async def process_txt_file(client: Client, message: Message):
    try:
        user_id = message.from_user.id
        if user_id not in AUTH_USERS:
            await message.reply_text("⚠️ You are not authorized to use this command.")
            return

        # Check if message is a reply
        if not message.reply_to_message:
            await message.reply_text(
                "⚠️ Please reply to a text file with /txt command.\n\n"
                "**Steps to use:**\n"
                "1. Send your .txt file containing URLs\n"
                "2. Reply to that file with /txt command\n\n"
                "**File Format:**\n"
                "`filename : url`\n"
                "(one URL per line)",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        replied = message.reply_to_message

        # If replied message has document
        if replied.document:
            if not replied.document.file_name.endswith('.txt'):
                await message.reply_text(
                    "⚠️ Only .txt files are supported!\n\n"
                    "Please make sure your file ends with .txt extension."
                )
                return
            
            # Download the txt file
            status_msg = await message.reply_text("📥 Downloading text file...")
            
            try:
                txt_path = await replied.download()
                if not txt_path:
                    await status_msg.edit_text("❌ Failed to download the txt file.")
                    return
                
                # Read URLs from file with proper encoding detection
                try:
                    encodings = ['utf-8', 'latin-1', 'ascii']
                    content = None
                    
                    for encoding in encodings:
                        try:
                            with open(txt_path, 'r', encoding=encoding) as f:
                                content = f.read()
                            break
                        except UnicodeDecodeError:
                            continue
                    
                    if content is None:
                        await status_msg.edit_text("❌ Could not read the text file. Invalid encoding.")
                        os.remove(txt_path)
                        return
                    
                    # Split content into lines and filter empty lines
                    urls = [line.strip() for line in content.splitlines() if line.strip()]
                    
                except Exception as e:
                    await status_msg.edit_text(f"❌ Failed to read the txt file: {str(e)}")
                    if os.path.exists(txt_path):
                        os.remove(txt_path)
                    return
                
                # Clean up txt file
                os.remove(txt_path)
                
                if not urls:
                    await status_msg.edit_text(
                        "❌ No valid URLs found in the text file.\n\n"
                        "**File Format:**\n"
                        "`filename : url`\n"
                        "(one URL per line)",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    return
                
                # Validate URL format
                valid_urls = []
                invalid_urls = []
                
                for line in urls:
                    if ":" in line:
                        filename, url = [x.strip() for x in line.split(":", 1)]
                        if filename and url:
                            valid_urls.append(line)
                        else:
                            invalid_urls.append(line)
                    else:
                        invalid_urls.append(line)
                
                if not valid_urls:
                    await status_msg.edit_text(
                        "❌ No valid URLs found in the text file.\n\n"
                        "**Required Format:**\n"
                        "`filename : url`\n\n"
                        "**Found Invalid Lines:**\n" +
                        "\n".join([f"- `{line}`" for line in invalid_urls[:5]]) +
                        ("\n..." if len(invalid_urls) > 5 else ""),
                        parse_mode=ParseMode.MARKDOWN
                    )
                    return
                
                # Initialize user state
                USER_STATES[user_id] = {
                    "state": "processing_txt",
                    "username": message.from_user.username or "Anonymous",
                    "batch_name": "Batch " + datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "canceled": False
                }
                
                # Create lock for this user
                if user_id not in update_locks:
                    update_locks[user_id] = threading.Lock()
                
                # Show batch processing status
                await status_msg.edit_text(
                    "🔄 Starting batch download...\n\n"
                    f"📚 Total valid URLs: {len(valid_urls)}\n"
                    f"⚠️ Invalid URLs: {len(invalid_urls)}\n\n"
                    "**Processing will start in 3 seconds...**\n\n"
                    "⚡ **Server Health Mode:**\n"
                    "• 10 second delay between files\n"
                    "• Helps prevent server overload\n"
                    "• Ensures stable processing",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_batch")]
                    ])
                )
                
                # Wait 3 seconds before starting
                await asyncio.sleep(3)
                
                success_count = 0
                failed_count = 0
                
                # Process each URL
                for i, line in enumerate(valid_urls, 1):
                    if USER_STATES[user_id].get("canceled", False):
                        await status_msg.edit_text(
                            "❌ Batch processing cancelled!\n\n"
                            f"📚 Total files: {len(valid_urls)}\n"
                            f"✅ Processed: {success_count}\n"
                            f"❌ Failed: {failed_count}\n"
                            f"⏹ Cancelled at: {i}/{len(valid_urls)}"
                        )
                        return
                    
                    # Update status with time estimate
                    remaining_files = len(valid_urls) - i
                    estimated_time = remaining_files * 10  # 10 seconds per file
                    hours, remainder = divmod(estimated_time, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    time_str = f"{int(hours)}h {int(minutes)}m {int(seconds)}s" if hours > 0 else f"{int(minutes)}m {int(seconds)}s"
                    
                    await status_msg.edit_text(
                        f"🔄 Processing file {i}/{len(valid_urls)}\n\n"
                        f"✅ Successful: {success_count}\n"
                        f"❌ Failed: {failed_count}\n"
                        f"⏳ Progress: {(i/len(valid_urls))*100:.1f}%\n\n"
                        f"⌛ Estimated time remaining: {time_str}\n"
                        "💡 Server Health Mode: 10s delay between files",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_batch")]
                        ])
                    )
                    
                    if await process_url_line(client, message, line, user_id):
                        success_count += 1
                    else:
                        failed_count += 1
                    
                    # Clean downloads directory after each file
                    clean_downloads_dir()
                    
                    # Add 10-second delay between files for server health
                    if i < len(valid_urls):  # Don't delay after the last file
                        logger.info("Waiting 10 seconds before next file (Server Health Mode)")
                        await status_msg.edit_text(
                            f"⏳ **Cooling Down...**\n\n"
                            f"• Processed: {i}/{len(valid_urls)} files\n"
                            f"• Waiting 10 seconds for server health\n"
                            f"• Next file starting soon...\n\n"
                            f"✅ Successful: {success_count}\n"
                            f"❌ Failed: {failed_count}",
                            parse_mode=ParseMode.MARKDOWN
                        )
                        await asyncio.sleep(10)
                
                # Final status
                await status_msg.edit_text(
                    "✅ Batch processing completed!\n\n"
                    f"📚 Total files: {len(valid_urls)}\n"
                    f"✅ Successfully processed: {success_count}\n"
                    f"❌ Failed: {failed_count}\n\n"
                    "Send another file or /start for a new session.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Start New Session", callback_data="start")]
                    ])
                )
                
            except Exception as e:
                logger.error(f"Error processing txt file: {e}")
                logger.error(traceback.format_exc())
                await status_msg.edit_text(
                    f"❌ An error occurred while processing: {str(e)}\n"
                    "Please try again or contact support."
                )
                
        else:
            # If replied message is not a document
            await message.reply_text(
                "⚠️ Please reply to a text (.txt) file.\n\n"
                "**Steps to use:**\n"
                "1. Send your .txt file containing URLs\n"
                "2. Reply to that file with /txt command\n\n"
                "**File Format:**\n"
                "`filename : url`\n"
                "(one URL per line)",
                parse_mode=ParseMode.MARKDOWN
            )
            
    except Exception as e:
        logger.error(f"Error in txt command: {e}")
        logger.error(traceback.format_exc())
        await message.reply_text(f"❌ An error occurred: {str(e)}")


# Add new callback handler for batch cancellation
@app.on_callback_query(filters.regex("^cancel_batch$"))
async def cancel_batch_download(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id in USER_STATES:
        USER_STATES[user_id]["canceled"] = True
        await callback_query.answer("Cancelling batch download...")
    else:
        await callback_query.answer("No active batch download found.")


@app.on_message(filters.document & filters.private)
async def handle_document(client: Client, message: Message):
    try:
        user_id = message.from_user.id
        if user_id not in AUTH_USERS:
            await message.reply_text("⚠️ You are not authorized to use this bot.")
            return

        # Check if it's a text file
        if not message.document.file_name.endswith('.txt'):
            return  # Silently ignore non-txt files
            
        # Get batch name from txt filename (remove .txt extension)
        batch_name = os.path.splitext(message.document.file_name)[0]
            
        # Download the txt file
        status_msg = await message.reply_text(
            "📥 **Processing Text File**\n\n"
            "⏳ Downloading and analyzing file...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        try:
            txt_path = await message.download()
            if not txt_path:
                await status_msg.edit_text("❌ Failed to download the txt file.")
                return
            
            # Read URLs from file with proper encoding detection
            try:
                encodings = ['utf-8', 'latin-1', 'ascii']
                content = None
                
                for encoding in encodings:
                    try:
                        with open(txt_path, 'r', encoding=encoding) as f:
                            content = f.read()
                        break
                    except UnicodeDecodeError:
                        continue
                
                if content is None:
                    await status_msg.edit_text("❌ Could not read the text file. Invalid encoding.")
                    os.remove(txt_path)
                    return
                
                # Split content into lines and filter empty lines
                urls = [line.strip() for line in content.splitlines() if line.strip()]
                
            except Exception as e:
                await status_msg.edit_text(f"❌ Failed to read the txt file: {str(e)}")
                if os.path.exists(txt_path):
                    os.remove(txt_path)
                return
            
            # Clean up txt file
            os.remove(txt_path)
            
            if not urls:
                await status_msg.edit_text(
                    "❌ No URLs found in the text file.\n\n"
                    "**Required Format:**\n"
                    "`filename : url`\n"
                    "(one URL per line)",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            # Validate URL format
            valid_urls = []
            invalid_urls = []
            
            await status_msg.edit_text(
                "🔍 **Validating URLs**\n\n"
                "⏳ Checking format and validity...",
                parse_mode=ParseMode.MARKDOWN
            )
            
            for line in urls:
                filename, url = parse_line(line)
                if filename and url:
                    valid_urls.append(line)
                else:
                    invalid_urls.append(line)
            
            if not valid_urls:
                invalid_examples = "\n".join([f"❌ `{line}`" for line in invalid_urls[:5]])
                await status_msg.edit_text(
                    "❌ **No Valid URLs Found**\n\n"
                    "**Required Format:**\n"
                    "`filename : url`\n\n"
                    f"**Invalid Lines Found:**\n{invalid_examples}"
                    + ("\n..." if len(invalid_urls) > 5 else ""),
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            # Initialize user state with txt filename as batch name
            USER_STATES[user_id] = {
                "state": "processing_txt",
                "username": message.from_user.first_name or message.from_user.username or "Anonymous",
                "batch_name": batch_name,
                "canceled": False
            }
            
            # Create lock for this user
            if user_id not in update_locks:
                update_locks[user_id] = threading.Lock()
            
            # Start processing immediately
            await status_msg.edit_text(
                f"✅ **Found {len(valid_urls)} Valid URLs**\n"
                f"⚠️ Skipped {len(invalid_urls)} Invalid URLs\n\n"
                "🔄 Starting batch download in 3 seconds...\n\n"
                "**Note:** Press /stop to cancel anytime",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel", callback_data="cancel_batch")]
                ])
            )
            
            # Wait 3 seconds before starting
            await asyncio.sleep(3)
            
            success_count = 0
            failed_count = 0
            
            # Process each URL
            for i, line in enumerate(valid_urls, 1):
                if USER_STATES[user_id].get("canceled", False):
                    await status_msg.edit_text(
                        "❌ **Batch Processing Cancelled**\n\n"
                        f"📊 **Progress Report:**\n"
                        f"• Total Files: `{len(valid_urls)}`\n"
                        f"• Processed: `{i-1}/{len(valid_urls)}`\n"
                        f"• Successful: `{success_count}`\n"
                        f"• Failed: `{failed_count}`\n"
                        f"• Completion: `{((i-1)/len(valid_urls))*100:.1f}%`",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    return
                
                await status_msg.edit_text(
                    f"🔄 **Batch Processing**\n\n"
                    f"📊 **Progress Report:**\n"
                    f"• Processing: `{i}/{len(valid_urls)}`\n"
                    f"• Successful: `{success_count}`\n"
                    f"• Failed: `{failed_count}`\n"
                    f"• Progress: `{(i/len(valid_urls))*100:.1f}%`\n\n"
                    "**Note:** Press /stop to cancel",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_batch")]
                    ])
                )
                
                if await process_url_line(client, message, line, user_id):
                    success_count += 1
                else:
                    failed_count += 1
                
                # Clean downloads directory after each file
                clean_downloads_dir()
                
                # Small delay between files
                await asyncio.sleep(1)
            
            # Final status
            await status_msg.edit_text(
                "✅ **Batch Processing Completed**\n\n"
                f"📊 **Final Report:**\n"
                f"• Total Files: `{len(valid_urls)}`\n"
                f"• Successful: `{success_count}`\n"
                f"• Failed: `{failed_count}`\n"
                f"• Success Rate: `{(success_count/len(valid_urls))*100:.1f}%`\n\n"
                "📤 Send another file or /start for new session",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Start New Session", callback_data="start")]
                ])
            )
            
        except Exception as e:
            logger.error(f"Error processing txt file: {e}")
            logger.error(traceback.format_exc())
            await status_msg.edit_text(
                "❌ **Processing Error**\n\n"
                f"Error: `{str(e)}`\n\n"
                "Please try again or contact support.",
                parse_mode=ParseMode.MARKDOWN
            )
            
    except Exception as e:
        logger.error(f"Error handling document: {e}")
        logger.error(traceback.format_exc())
        await message.reply_text(
            "❌ **An Error Occurred**\n\n"
            f"Error: `{str(e)}`",
            parse_mode=ParseMode.MARKDOWN
        )


# Add this new handler for custom thumbnails
@app.on_message(filters.photo & filters.private)
async def save_thumbnail(client: Client, message: Message):
    """Save custom thumbnail with enhanced handling"""
    try:
        user_id = message.from_user.id
        if user_id not in AUTH_USERS:
            await message.reply_text("⚠️ You are not authorized to use this bot.")
            return

        # Create downloads directory if it doesn't exist
        if not os.path.exists("downloads"):
            os.makedirs("downloads")

        # Download the photo
        thumb_path = f"downloads/thumb_{user_id}.jpg"
        
        # Remove old thumbnail if exists
        if os.path.exists(thumb_path):
            os.remove(thumb_path)
        
        # Download and process new thumbnail
        await message.download(file_name=thumb_path)

        # Optimize thumbnail
        try:
            img = Image.open(thumb_path)
            # Convert to RGB if necessary
            if img.mode != 'RGB':
                img = img.convert('RGB')
            # Resize maintaining aspect ratio
            img.thumbnail((320, 320), Image.Resampling.LANCZOS)
            # Save with optimal settings
            img.save(thumb_path, "JPEG", quality=95, optimize=True)
            
            # Verify the thumbnail
            with Image.open(thumb_path) as verify_img:
                verify_img.verify()
            
            # Store thumbnail path for user
            USER_THUMBNAILS[user_id] = thumb_path
            
            # Send confirmation with thumbnail preview
            await message.reply_photo(
                photo=thumb_path,
                caption=(
                    "✅ **Custom Thumbnail Saved Successfully!**\n\n"
                    "This thumbnail will be used for all your future uploads.\n"
                    "• Send another photo to update it\n"
                    "• Use /delthumbnail to remove it"
                ),
                parse_mode=ParseMode.MARKDOWN
            )
            
            logger.info(f"Custom thumbnail saved for user {user_id}")
            
        except Exception as e:
            logger.error(f"Error processing thumbnail: {e}")
            if os.path.exists(thumb_path):
                os.remove(thumb_path)
            await message.reply_text(
                "❌ Failed to process thumbnail image.\n"
                "Please make sure you're sending a valid image file."
            )
            return

    except Exception as e:
        logger.error(f"Error saving thumbnail: {e}")
        await message.reply_text(
            "❌ Failed to save thumbnail.\n"
            "Please try again or contact support."
        )

@app.on_message(filters.command("delthumbnail") & filters.private)
async def delete_thumbnail(client: Client, message: Message):
    """Delete custom thumbnail with enhanced handling"""
    try:
        user_id = message.from_user.id
        if user_id not in AUTH_USERS:
            await message.reply_text("⚠️ You are not authorized to use this bot.")
            return

        if user_id in USER_THUMBNAILS:
            thumb_path = USER_THUMBNAILS[user_id]
            if os.path.exists(thumb_path):
                os.remove(thumb_path)
                logger.info(f"Deleted thumbnail file: {thumb_path}")
            del USER_THUMBNAILS[user_id]
            await message.reply_text(
                "✅ **Custom Thumbnail Removed!**\n\n"
                "• Bot will generate thumbnails automatically\n"
                "• Send any photo to set a new custom thumbnail",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await message.reply_text(
                "ℹ️ **No Custom Thumbnail Found!**\n\n"
                "Send any photo to set a custom thumbnail.",
                parse_mode=ParseMode.MARKDOWN
            )

    except Exception as e:
        logger.error(f"Error deleting thumbnail: {e}")
        await message.reply_text(
            "❌ Failed to remove thumbnail.\n"
            "Please try again or contact support."
        )


# Add this new help command handler
@app.on_message(filters.command("help"))
async def help_command(client: Client, message: Message):
    try:
        help_text = (
            "🤖 **URL Uploader Bot Help**\n"
            "➖➖➖➖➖➖➖➖➖➖\n\n"
            "**Available Commands:**\n\n"
            "📌 **Basic Commands:**\n"
            "• `/start` - Start the bot\n"
            "• `/help` - Show this help message\n"
            "• `/stop` - Stop current process\n\n"
            
            "🖼️ **Thumbnail Commands:**\n"
            "• Send any photo to set as thumbnail\n"
            "• `/delthumbnail` - Remove custom thumbnail\n\n"
            
            "📤 **Upload Methods:**\n"
            "1. **Single URL:**\n"
            "   • Send URL in format:\n"
            "   • `filename : https://example.com/file.mp4`\n"
            "   • `filename : https://example.com/stream.m3u8`\n\n"
            
            "2. **Batch Upload (Text File):**\n"
            "   • Send a .txt file containing URLs\n"
            "   • Format: `filename : URL` (one per line)\n"
            "   • Bot will process all URLs automatically\n\n"
            
            "📝 **Supported Formats:**\n"
            "• Videos: MKV, MP4, AVI, MOV, M3U8 (Streaming)\n"
            "• Documents: PDF\n"
            "• Encrypted Videos: `*.mkv*key`\n\n"
            
            "⚡ **Features:**\n"
            "• High-quality video uploads\n"
            "• Streaming URL support (HLS/M3U8)\n"
            "• Custom thumbnail support\n"
            "• Real-time progress tracking\n"
            "• Batch processing\n"
            "• Auto-generated thumbnails\n"
            "• Supports encrypted videos\n\n"
            
            "👨‍💻 **Developer:** @MrGadhvii\n"
            "📢 **Channel:** @MrGadhvii\n\n"
            
            "🔐 **Note:** This is a private bot.\n"
            "Contact admin for authorization.\n"
            "➖➖➖➖➖➖➖➖➖➖"
        )

        # Check if user is authorized
        is_authorized = message.from_user.id in AUTH_USERS
        
        # Different button layouts based on authorization
        if is_authorized:
            buttons = [
                [InlineKeyboardButton("👨‍💻 Developer", url="https://t.me/MrGadhvii")],
                [InlineKeyboardButton("📢 Updates Channel", url="https://t.me/MrGadhvii")],
                [InlineKeyboardButton("🔙 Back to Menu", callback_data="start")]
            ]
        else:
            buttons = [
                [InlineKeyboardButton("👨‍💻 Developer", url="https://t.me/MrGadhvii")],
                [InlineKeyboardButton("📢 Updates Channel", url="https://t.me/MrGadhvii")],
                [InlineKeyboardButton("🔐 Request Access", url="https://t.me/MrGadhvii")]
            ]

        await message.reply_text(
            help_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    except Exception as e:
        logger.error(f"Error in help command: {e}")
        await message.reply_text(
            "❌ An error occurred while showing help.\n"
            "Please try again later."
        )


def clean_logs():
    """Clean log files and terminal output"""
    try:
        # Clear terminal (platform independent)
        os.system('cls' if os.name == 'nt' else 'clear')
        
        # Clean log files
        if os.path.exists(log_file):
            with open(log_file, 'w') as f:
                f.truncate(0)
        
        # Clean any rotated log files
        for old_log in glob.glob(f"{log_file}.*"):
            try:
                os.remove(old_log)
            except:
                pass
                
        logger.info("Logs cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning logs: {e}")


async def format_duration(seconds):
    """Format duration in a modern way"""
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

async def get_video_info(video_path):
    """Get video metadata using multiple methods to ensure accuracy"""
    try:
        logger.info(f"Getting video info for: {video_path}")
        
        # Enhanced handling for encrypted videos
        if '*' in video_path:
            logger.info("Encrypted video detected, waiting 8 seconds for decryption...")
            await asyncio.sleep(8)  # Increased initial delay for decryption
            max_attempts = 8  # More attempts for encrypted files
        else:
            max_attempts = 5
        
        width = height = duration = None
        
        for attempt in range(max_attempts):
            try:
                # Method 1: Try ffprobe with detailed video stream analysis
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
                    
                    # Get video stream info
                    stream_info = probe.get('streams', [{}])[0]
                    format_info = probe.get('format', {})
                    
                    # Calculate duration from multiple sources
                    duration_sources = [
                        float(stream_info.get('duration', 0)),
                        float(format_info.get('duration', 0)),
                        await get_duration_ffmpeg(video_path)  # Fallback to ffmpeg
                    ]
                    duration = max(d for d in duration_sources if d > 0)
                    
                    # Get dimensions
                    width = int(stream_info.get('width', 1280))
                    height = int(stream_info.get('height', 720))
                    
                    # For encrypted videos, add extra validation
                    if '*' in video_path:
                        if duration <= 0 or width <= 0 or height <= 0:
                            raise ValueError("Invalid metadata from encrypted file")
                    
                    # Scale dimensions properly
                    if width < 1280:
                        scale_factor = 1280 / width
                        width = 1280
                        height = int(height * scale_factor)
                    
                    # Ensure even dimensions
                    width = width // 2 * 2
                    height = height // 2 * 2
                    
                    logger.info(f"Video metadata: {width}x{height}, {duration}s")
                    return width, height, int(duration)
                
                # If ffprobe failed, try alternative method with ffmpeg
                logger.info("Falling back to ffmpeg for metadata extraction...")
                duration = await get_duration_ffmpeg(video_path)
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
                
                # Extract dimensions from ffmpeg output
                dimension_match = re.search(r'(\d{3,4})x(\d{3,4})', stderr)
                if dimension_match:
                    width, height = map(int, dimension_match.groups())
                
                if width and height and duration:
                    return width, height, int(duration)
                
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
                
            except Exception as e:
                logger.error(f"Attempt {attempt+1} failed: {str(e)}")
                if attempt == max_attempts - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
        
        # Final fallback for encrypted videos
        return 1280, 720, int(duration if duration else 60)
    
    except Exception as e:
        logger.error(f"Video info error: {str(e)}")
        return 1280, 720, 60

async def get_duration_ffmpeg(video_path):
    """Get video duration using ffmpeg as fallback"""
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
        
        stdout, stderr = await process.communicate()
        stderr = stderr.decode()
        
        # Try to extract duration from ffmpeg output
        duration_match = re.search(r"Duration: (\d{2}):(\d{2}):(\d{2})", stderr)
        if duration_match:
            hours, minutes, seconds = map(int, duration_match.groups())
            duration = hours * 3600 + minutes * 60 + seconds
            logger.info(f"Got duration from ffmpeg: {duration}s")
            return duration
            
        return 60
        
    except Exception as e:
        logger.error(f"Error getting duration with ffmpeg: {e}")
        return 60

async def generate_thumbnail(video_path, timestamp=1):
    """Generate thumbnail with enhanced handling for encrypted videos"""
    try:
        thumbnail_path = f"{video_path}_thumb.jpg"
        max_attempts = 8 if '*' in video_path else 5
        
        for attempt in range(max_attempts):
            try:
                current_ts = timestamp + (attempt * 5)  # Try different timestamps
                cmd = [
                    'ffmpeg',
                    '-ss', str(current_ts),
                    '-i', video_path,
                    '-vframes', '1',
                    '-vf', 'thumbnail,scale=320:320:force_original_aspect_ratio=decrease',
                    '-y', thumbnail_path
                ]
                
                # Longer timeout for encrypted videos
                process = await asyncio.create_subprocess_exec(*cmd)
                await asyncio.wait_for(process.communicate(), timeout=30 if '*' in video_path else 15)
                
                if os.path.exists(thumbnail_path):
                    with Image.open(thumbnail_path) as img:
                        img.verify()
                        img.thumbnail((320, 320))
                        img.save(thumbnail_path, "JPEG", quality=95)
                    return thumbnail_path
                
            except Exception as e:
                logger.error(f"Thumbnail attempt {attempt+1} failed: {str(e)}")
                if attempt == max_attempts - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
        
        return None
    except Exception as e:
        logger.error(f"Thumbnail generation failed: {str(e)}")
        return None

def get_file_extension_from_url(url):
    """Extract file extension from URL with better PDF detection"""
    try:
        # Log original URL for debugging
        logger.info(f"Extracting extension from URL: {url}")
        
        # Handle URL formats with prefixes like TEST: or @
        if ':' in url and not url.startswith('http'):
            parts = url.split(':', 1)
            if len(parts) == 2 and 'http' in parts[1]:
                url = parts[1].strip()
                logger.info(f"Removed prefix, new URL: {url}")
        
        # Remove @ prefix if present
        if url.startswith('@'):
            url = url[1:].strip()
            logger.info(f"Removed @ prefix, new URL: {url}")
            
        # Remove query parameters and decryption key
        clean_url = url.split('?')[0].split('*')[0].lower().strip()
        logger.info(f"Cleaned URL: {clean_url}")
        
        # STRICT PDF DETECTION - Multiple checks
        # 1. Check for .pdf extension
        if clean_url.endswith('.pdf'):
            logger.info("PDF detected by .pdf extension")
            return '.pdf'
            
        # 2. Check for PDF in path components
        pdf_indicators = ['/pdf/', 'pdf/', '/pdf', 'application/pdf', 'content-type=pdf']
        if any(x in clean_url for x in pdf_indicators):
            logger.info(f"PDF detected by path indicator: {[x for x in pdf_indicators if x in clean_url]}")
            return '.pdf'
            
        # 3. Check for PDF in filename without extension
        if clean_url.split('/')[-1].lower() == 'pdf':
            logger.info("PDF detected by filename 'pdf'")
            return '.pdf'
            
        # 4. Check URL for common PDF hosting patterns
        pdf_hosts = ['docs.google.com/document', 'dropbox.com/pdf', 'drive.google.com']
        if any(host in clean_url for host in pdf_hosts) and 'pdf' in clean_url:
            logger.info(f"PDF detected by hosting service: {[x for x in pdf_hosts if x in clean_url]}")
            return '.pdf'
        
        # Extract normal extension from filename in URL
        url_parts = clean_url.split('/')
        if url_parts:
            filename = url_parts[-1]
            if '.' in filename:
                ext = '.' + filename.split('.')[-1].lower()
                if len(ext) <= 5 and ext[1:].isalnum():  # Valid extension check
                    logger.info(f"Found extension from URL filename: {ext}")
                    return ext
        
        # Last resort check for PDF content
        if 'pdf' in clean_url:
            logger.info("PDF detected by 'pdf' keyword in URL")
            return '.pdf'
            
        logger.warning(f"No extension could be determined from URL: {url}")
        return None
    except Exception as e:
        logger.error(f"Error extracting extension from URL: {e}")
        # Safety fallback for PDF
        if 'pdf' in url.lower():
            return '.pdf'
        return None

def ensure_filename_has_extension(filename, url, file_type=None):
    """Ensure filename has the correct extension with robust PDF handling"""
    try:
        logger.info(f"Processing filename: '{filename}', URL: '{url}', type: '{file_type}'")
        
        # PDF detection comes first - multiple checks
        is_pdf = False
        
        # Check URL for PDF indicators (highest priority)
        if url:
            # Strong PDF indicator check
            pdf_strong_indicators = ['.pdf', '/pdf/', 'pdf/', '/pdf', 'application/pdf']
            if any(indicator in url.lower() for indicator in pdf_strong_indicators):
                logger.info(f"PDF detected by strong indicator in URL")
                is_pdf = True
                
            # PDF keyword check
            elif 'pdf' in url.lower():
                logger.info("PDF detected by 'pdf' keyword in URL")
                is_pdf = True
                
        # Check file_type parameter
        if file_type and 'pdf' in file_type.lower():
            logger.info("PDF detected by file_type parameter")
            is_pdf = True
            
        # Get filename parts
        filename_base, filename_ext = os.path.splitext(filename)
        filename_ext = filename_ext.lower() if filename_ext else ''
        logger.info(f"Filename base: '{filename_base}', extension: '{filename_ext}'")
        
        # If it's a PDF, ensure PDF extension
        if is_pdf:
            logger.info("Setting .pdf extension")
            return f"{filename_base}.pdf"
            
        # If filename already has a PDF extension, keep it
        if filename_ext == '.pdf':
            logger.info("Keeping existing .pdf extension")
            return filename
            
        # Try to get extension from URL
        url_ext = get_file_extension_from_url(url) if url else None
        logger.info(f"Extension from URL: {url_ext}")
        
        # Use URL extension if available
        if url_ext:
            logger.info(f"Using extension from URL: {url_ext}")
            return f"{filename_base}{url_ext}"
            
        # If filename already has a valid extension, keep it
        if filename_ext and len(filename_ext) <= 5 and filename_ext[1:].isalnum():
            logger.info(f"Keeping existing valid extension: {filename_ext}")
            return filename
            
        # Last resort for PDFs: if URL contains 'pdf' add .pdf extension
        if url and 'pdf' in url.lower():
            logger.info("Adding .pdf extension based on URL containing 'pdf'")
            return f"{filename_base}.pdf"
            
        logger.warning(f"No extension could be determined, using original filename: {filename}")
        return filename
        
    except Exception as e:
        logger.error(f"Error ensuring filename extension: {e}")
        # In case of any error with PDFs, ensure .pdf extension
        if url and 'pdf' in url.lower():
            logger.info("Fallback: Adding .pdf extension after error")
            if filename.lower().endswith('.pdf'):
                return filename
            return f"{filename}.pdf"
        return filename

# Add this function near other cleanup functions
def clean_all_files():
    """Clean all temporary files including thumbnails"""
    try:
        # Clean downloads directory
        if os.path.exists("downloads"):
            for file in os.listdir("downloads"):
                try:
                    file_path = os.path.join("downloads", file)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                        logger.info(f"Removed file: {file_path}")
                except Exception as e:
                    logger.error(f"Error removing file {file}: {e}")
        
        # Clean any thumb files in current directory
        for file in os.listdir():
            if file.endswith("_thumb.jpg") or "thumb" in file.lower():
                try:
                    os.remove(file)
                    logger.info(f"Removed thumbnail: {file}")
                except Exception as e:
                    logger.error(f"Error removing thumbnail {file}: {e}")
                    
    except Exception as e:
        logger.error(f"Error in cleanup: {e}")

@app.on_message(filters.command("filter") & filters.private)
async def filter_text_file(client: Client, message: Message):
    try:
        # Check authorization
        user_id = message.from_user.id
        if user_id not in AUTH_USERS:
            await message.reply_text("⚠️ You are not authorized to use this command.")
            return

        # Check if message is a reply to a text file
        if not message.reply_to_message or not message.reply_to_message.document:
            await message.reply_text(
                "⚠️ Please reply to a text file with /filter command.\n\n"
                "This command helps format imperfect URL lists into the correct format."
            )
            return

        replied = message.reply_to_message
        if not replied.document.file_name.endswith('.txt'):
            await message.reply_text("⚠️ Only .txt files are supported!")
            return

        # Download the text file
        status_msg = await message.reply_text("📝 Processing text file...")
        
        try:
            file_path = await replied.download()
            if not file_path:
                await status_msg.edit_text("❌ Failed to download the text file.")
                return

            # Process the file
            success, result = await process_text_file(file_path)
            
            if success:
                stats = result
                await status_msg.edit_text(
                    f"✅ **File Processed Successfully!**\n\n"
                    f"📊 **Statistics:**\n"
                    f"• Total Lines: {stats['total']}\n"
                    f"• Formatted: {stats['formatted']}\n"
                    f"• Skipped: {stats['skipped']}\n\n"
                    "📤 Sending formatted file..."
                )
                
                # Send the formatted file
                await message.reply_document(
                    document=stats['output_path'],
                    caption=(
                        "📝 **Formatted URL List**\n\n"
                        f"• Total URLs: {stats['formatted']}\n"
                        "• Format: `filename : url`\n\n"
                        "Reply with /txt to process this file."
                    ),
                    parse_mode=ParseMode.MARKDOWN
                )
                
                # Clean up
                os.remove(file_path)
                os.remove(stats['output_path'])
                
            else:
                await status_msg.edit_text(f"❌ Failed to process file: {result}")
                if os.path.exists(file_path):
                    os.remove(file_path)
                    
        except Exception as e:
            await status_msg.edit_text(f"❌ An error occurred: {str(e)}")
            if os.path.exists(file_path):
                os.remove(file_path)
                
    except Exception as e:
        await message.reply_text(f"❌ An error occurred: {str(e)}")

# Start the bot
if __name__ == "__main__":
    logger.info("Starting URL Uploader Bot...")
    app.run()
