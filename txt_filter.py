import os
import re
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clean_filename(filename):
    """Clean and format filename"""
    # Remove invalid characters
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)
    # Remove multiple spaces
    filename = ' '.join(filename.split())
    # Remove leading/trailing spaces and dots
    filename = filename.strip('. ')
    return filename or 'File'

def clean_url(url):
    """Clean and validate URL"""
    # Remove leading/trailing whitespace
    url = url.strip()
    # Ensure URL starts with http:// or https://
    if not url.startswith(('http://', 'https://')):
        if '//' in url:
            url = 'https:' + url[url.index('//'):]
        else:
            url = 'https://' + url
    return url

def format_line(line):
    """Format a single line into the correct format"""
    try:
        # Skip empty lines
        if not line.strip():
            return None
            
        # Common patterns to handle
        patterns = [
            # Pattern 1: filename - url
            r'^(.+?)\s*-\s*(https?://\S+|//\S+|\S+\.\S+)$',
            # Pattern 2: filename : url with multiple colons
            r'^(.+?)\s*:+\s*(https?://\S+|//\S+|\S+\.\S+)$',
            # Pattern 3: url only (generate filename from URL)
            r'^(https?://\S+|//\S+|\S+\.\S+)$',
            # Pattern 4: filename followed by url
            r'^(.+?)\s+(https?://\S+|//\S+|\S+\.\S+)$'
        ]
        
        for pattern in patterns:
            match = re.match(pattern, line.strip())
            if match:
                if len(match.groups()) == 1:
                    # URL only - extract filename from URL
                    url = match.group(1)
                    url = clean_url(url)
                    filename = url.split('/')[-1].split('?')[0].split('*')[0]
                    if not filename:
                        filename = f"File_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                else:
                    filename = match.group(1)
                    url = match.group(2)
                
                # Clean filename and URL
                filename = clean_filename(filename)
                url = clean_url(url)
                
                # Add extension from URL if missing
                if url.lower().endswith('.pdf') and not filename.lower().endswith('.pdf'):
                    filename += '.pdf'
                elif '*' in url and '.mkv' in url.lower() and not filename.lower().endswith('.mkv'):
                    filename += '.mkv'
                elif any(url.lower().endswith(ext) for ext in ['.mp4', '.mkv', '.avi']):
                    ext = url.split('.')[-1].split('*')[0].lower()
                    if not filename.lower().endswith(ext):
                        filename += f'.{ext}'
                
                return f"{filename} : {url}"
        
        # If no pattern matches but line contains a URL
        url_match = re.search(r'(https?://\S+|//\S+|\S+\.\S+)', line)
        if url_match:
            url = clean_url(url_match.group(1))
            remaining = line.replace(url_match.group(1), '').strip(' :-')
            filename = clean_filename(remaining) if remaining else url.split('/')[-1].split('?')[0]
            return f"{filename} : {url}"
            
        return None
        
    except Exception as e:
        logger.error(f"Error formatting line: {str(e)}")
        return None

async def process_text_file(input_path):
    """Process text file and return formatted content"""
    try:
        # Read input file with different encodings
        content = None
        encodings = ['utf-8', 'latin-1', 'ascii']
        
        for encoding in encodings:
            try:
                with open(input_path, 'r', encoding=encoding) as f:
                    content = f.read()
                break
            except UnicodeDecodeError:
                continue
        
        if content is None:
            raise ValueError("Could not read file with any supported encoding")
        
        # Process lines
        formatted_lines = []
        skipped_lines = []
        
        for line in content.splitlines():
            if line.strip():
                formatted = format_line(line)
                if formatted:
                    formatted_lines.append(formatted)
                else:
                    skipped_lines.append(line)
        
        # Generate output content
        output_content = []
        
        if formatted_lines:
            output_content.append("✅ Formatted URLs:")
            output_content.extend(formatted_lines)
            
        if skipped_lines:
            if output_content:
                output_content.append("\n❌ Skipped Lines (Invalid Format):")
            output_content.extend(skipped_lines)
        
        # Create output file path
        output_dir = "downloads"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(output_dir, f"formatted_{timestamp}.txt")
        
        # Write formatted content
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(output_content))
        
        stats = {
            'total': len(formatted_lines) + len(skipped_lines),
            'formatted': len(formatted_lines),
            'skipped': len(skipped_lines),
            'output_path': output_path
        }
        
        return True, stats
        
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        return False, str(e)

# Example usage in bot.py:
"""
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
""" 