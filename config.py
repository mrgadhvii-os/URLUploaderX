import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Bot Configuration
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Admin Configuration
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))  # Default to 0 if not set
OWNER_ID = int(os.getenv("OWNER_ID", "0"))  # Default to 0 if not set

# Initialize authorized users list with admin and owner
AUTH_USERS = [ADMIN_ID, OWNER_ID]

# Add additional authorized users
auth_users_str = os.getenv('AUTH_USERS', '')
if auth_users_str:
    # Handle both comma and space separated values
    # First split by comma, then by space, and flatten the list
    try:
        for part in auth_users_str.split(','):
            # Split each part by space and process
            for user_id in part.split():
                try:
                    user_id = int(user_id.strip())
                    if user_id not in AUTH_USERS:  # Avoid duplicates
                        AUTH_USERS.append(user_id)
                except ValueError:
                    print(f"Warning: Invalid user ID '{user_id}' in AUTH_USERS")
                    continue
    except Exception as e:
        print(f"Warning: Error processing AUTH_USERS: {e}")

# Remove duplicates and ensure admin and owner are included
AUTH_USERS = list(set(AUTH_USERS))
if ADMIN_ID not in AUTH_USERS and ADMIN_ID != 0:
    AUTH_USERS.append(ADMIN_ID)
if OWNER_ID not in AUTH_USERS and OWNER_ID != 0:
    AUTH_USERS.append(OWNER_ID)

# Remove invalid IDs (0 or negative)
AUTH_USERS = [x for x in AUTH_USERS if x > 0]

# Print authorized users for verification
print(f"Authorized Users: {AUTH_USERS}")

# User Configuration
OWNER_ID = int(os.getenv("OWNER_ID"))

# Worker Configuration
WORKERS = int(os.getenv("WORKERS", "6"))

# Database Configuration
DATABASE_URL = os.getenv("DATABASE_URL")

# Download Configuration
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True) 