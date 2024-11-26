import os
import logging
import math
import json
import sqlite3
from datetime import datetime
import random
import requests

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
def divide_file_into_chunks(file_path, file_id, master_node_id,chunk_size_mb=2, replication_factor=3):
    DB_FILE = f"database/{master_node_id}_metadata.db"
    chunk_size = chunk_size_mb * 1024 * 1024
    file_size = os.path.getsize(file_path)
    num_chunks = math.ceil(file_size / chunk_size)
    chunks_info = []

    # Fetch active workers
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT worker_id FROM workers WHERE status = "active"')
    active_workers = [row[0] for row in cursor.fetchall()]
    conn.close()

    if len(active_workers) < replication_factor:
        raise Exception("Not enough active workers to replicate chunks")

    with open(file_path, 'rb') as file:
        for i in range(num_chunks):
            chunk_data = file.read(chunk_size)
            chunk_id = f"{file_id}_chunk_{i+1}"
            assigned_workers = random.sample(active_workers, replication_factor)

            for worker_id in assigned_workers:
                try:
                    worker_url = f"http://127.0.0.1:{get_worker_port(worker_id)}/chunks/{chunk_id}"
                    response = requests.post(worker_url, data=chunk_data)
                    response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    raise Exception(f"Failed to store chunk {chunk_id} on worker {worker_id}: {e}")

            chunks_info.append({'chunk_id': chunk_id, 'size': len(chunk_data), 'worker_ids': assigned_workers})

    return chunks_info

