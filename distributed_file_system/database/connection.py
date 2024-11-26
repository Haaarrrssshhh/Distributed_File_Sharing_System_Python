import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# MongoDB URI from environment variables
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise Exception("MONGO_URI not found in environment variables")

# MongoDB Database Name
DB_NAME = os.getenv("DB_NAME", "prod")

# Establish connection to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Export the database connection
def get_database():
    """
    Returns the MongoDB database instance.
    """
    return db
