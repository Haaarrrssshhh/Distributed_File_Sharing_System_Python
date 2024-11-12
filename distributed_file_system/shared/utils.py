import os
import logging
import math
import json
from datetime import datetime
import random

# Logger setup
LOG_FILE = 'logs/api_gateway.log'

if not os.path.exists('logs'):
    os.makedirs('logs')

logging.basicConfig(
    filename=os.path.join(LOG_FILE),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Worker storage directories (update as needed)
WORKER_STORAGE_PATHS = {
    "worker_1": "storage/worker_1",
    "worker_2": "storage/worker_2",
    "worker_3": "storage/worker_3",
    "worker_4": "storage/worker_4",
    "worker_5": "storage/worker_5"
}

# Ensure worker directories exist
for worker_path in WORKER_STORAGE_PATHS.values():
    os.makedirs(worker_path, exist_ok=True)

# Function to log API calls
def log_api_call(action, file_id, details):
    """
    Logs API calls with details of the action performed.

    Args:
        action (str): Action performed (CREATE, READ, DELETE).
        file_id (str): ID of the file the action was performed on.
        details (dict): Additional details of the action.
    """
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'action': action,
        'file_id': file_id,
        'details': details
    }
    logging.info(json.dumps(log_entry, indent=2))


# Function to divide a file into chunks and store in worker directories
def divide_file_into_chunks(file_path, file_id, chunk_size_mb=128, replication_factor=3):
    """
    Divides a file into chunks and stores them in worker directories.

    Args:
        file_path (str): Path to the file to be divided.
        file_id (str): ID of the file being divided.
        chunk_size_mb (int): Size of each chunk in MB.
        replication_factor (int): Number of copies for each chunk.

    Returns:
        list: Metadata about the chunks (chunk ID, size, worker IDs).
    """
    chunk_size = chunk_size_mb * 1024 * 1024  # Convert MB to bytes
    file_size = os.path.getsize(file_path)
    num_chunks = math.ceil(file_size / chunk_size)
    chunks_info = []

    # Read file and divide into chunks
    with open(file_path, 'rb') as file:
        for i in range(num_chunks):
            start = i * chunk_size
            file.seek(start)
            chunk_data = file.read(chunk_size)

            # Generate chunk ID
            chunk_id = f"{file_id}_chunk_{i+1}"

            # Select worker nodes for replication
            assigned_workers = random.sample(list(WORKER_STORAGE_PATHS.keys()), replication_factor)

            # Store chunk in each assigned worker directory
            for worker_id in assigned_workers:
                worker_path = os.path.join(WORKER_STORAGE_PATHS[worker_id], chunk_id)
                with open(worker_path, 'wb') as chunk_file:
                    chunk_file.write(chunk_data)

            # Add chunk metadata
            chunks_info.append({
                'chunk_id': chunk_id,
                'size': len(chunk_data),
                'worker_ids': assigned_workers
            })

    return chunks_info
