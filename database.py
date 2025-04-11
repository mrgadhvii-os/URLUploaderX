import time
from motor.motor_asyncio import AsyncIOMotorClient
from config import DATABASE_URL

class Database:
    def __init__(self):
        self.client = AsyncIOMotorClient(DATABASE_URL, serverSelectionTimeoutMS=5000)
        self.db = self.client.url_uploader
        self.users = self.db.users
        self.downloads = self.db.downloads

    async def add_user(self, user_id: int, username: str, batch_name: str):
        try:
            await self.users.update_one(
                {"user_id": user_id},
                {"$set": {"username": username, "batch_name": batch_name}},
                upsert=True
            )
            return True
        except Exception as e:
            print(f"Database error in add_user: {e}")
            return False

    async def get_user(self, user_id: int):
        try:
            return await self.users.find_one({"user_id": user_id})
        except Exception as e:
            print(f"Database error in get_user: {e}")
            return None

    async def add_download(self, user_id: int, filename: str, url: str):
        try:
            return await self.downloads.insert_one({
                "user_id": user_id,
                "filename": filename,
                "url": url,
                "status": "pending",
                "timestamp": time.time()
            })
        except Exception as e:
            print(f"Database error in add_download: {e}")
            return None

    async def update_download_status(self, download_id, status: str):
        try:
            await self.downloads.update_one(
                {"_id": download_id},
                {"$set": {"status": status, "updated_at": time.time()}}
            )
            return True
        except Exception as e:
            print(f"Database error in update_download_status: {e}")
            return False

# Create a single instance
db = Database() 
