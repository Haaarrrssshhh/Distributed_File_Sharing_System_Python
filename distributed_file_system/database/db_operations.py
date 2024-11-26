from database.connection import get_database
from datetime import datetime, timedelta

# Get the database instance
db = get_database()

# Reusable functions for accessing collections
def get_workers_collection():
    """
    Returns the workers collection.
    """
    return db["workers"]

def get_files_collection():
    """
    Returns the files collection.
    """
    return db["files"]

def get_metadata_collection():
    """
    Returns the metadata collection.
    """
    return db["metadata"]

# Utility to update worker information
def update_worker(worker_id, url, status="active"):
    """
    Updates or inserts worker information in the workers collection.
    """
    workers = get_workers_collection()
    workers.update_one(
        {"worker_id": worker_id},
        {"$set": {
            "url": url,
            "status": status,
            "last_heartbeat": datetime.utcnow()
        }},
        upsert=True
    )

# Utility to get active workers
def get_active_workers():
    """
    Retrieves all active workers.
    """
    workers = get_workers_collection()
    return list(workers.find({"status": "active"}, {"_id": 0, "worker_id": 1, "url": 1}))

# Utility to store file metadata
def store_file_metadata(file_id, file_name, size, chunks):
    files = get_files_collection()
    print(file_id,"file_id")
    files.insert_one({
        "file_id": file_id,
        "file_name": file_name,
        "size": size,
        "chunks": chunks,
        "status": "active",
        "created_at": datetime.utcnow()
    })

# Utility to fetch file metadata
def fetch_file_metadata(file_id):
    """
    Fetches metadata for a file from the files collection.
    """
    files = get_files_collection()
    return files.find_one({"file_id": file_id})

# Utility to update leader metadata
def update_leader_metadata(leader_id):
    """
    Updates leader information in the metadata collection.
    """
    metadata = get_metadata_collection()
    metadata.update_one(
        {"type": "leader"},
        {"$set": {
            "leader": leader_id,
            "last_updated": datetime.utcnow()
        }},
        upsert=True
    )

# Utility to fetch leader metadata
def fetch_leader_metadata():
    """
    Fetches leader information from the metadata collection.
    """
    metadata = get_metadata_collection()
    return metadata.find_one({"type": "leader"})



def mark_inactive_workers(timeout_seconds):
    """
    Marks workers as inactive if they have not sent a heartbeat within the timeout period.
    """
    workers = get_workers_collection()
    timeout_threshold = datetime.utcnow() - timedelta(seconds=timeout_seconds)
    result = workers.update_many(
        {
            "last_heartbeat": {"$lt": timeout_threshold},
            "status": "active"
        },
        {
            "$set": {"status": "inactive"}
        }
    )
    return result.modified_count  # Number of workers marked as inactive
