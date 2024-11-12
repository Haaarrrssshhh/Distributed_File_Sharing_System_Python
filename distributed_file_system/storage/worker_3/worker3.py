from flask import Flask, request, jsonify, send_file
import os
import requests
import threading
import time
from datetime import datetime

app = Flask(__name__)

# Worker Node Configuration
WORKER_ID = "worker_3"  # Change for each worker instance
STORAGE_DIR = f"distributed_file_system/storage/{WORKER_ID}"
MASTER_NODE_URL = "http://127.0.0.1:5001"  # Update if Master Node runs on a different host/port
HEARTBEAT_INTERVAL = 10  # In seconds

# Ensure storage directory exists
os.makedirs(STORAGE_DIR, exist_ok=True)

# Heartbeat to Master Node
def send_heartbeat():
    """
    Sends periodic heartbeats to the Master Node.
    """
    while True:
        try:
            # Send heartbeat request
            response = requests.post(f"{MASTER_NODE_URL}/heartbeat/{WORKER_ID}")
            
            # Debugging the exact response status and content
            print(f"DEBUG: Status Code: {response.status_code}, Response Body: {response.text}")
            
            if response.status_code == 200:
                print(f"[{datetime.now()}] Heartbeat sent successfully: {response.text}")
            else:
                print(f"[{datetime.now()}] Heartbeat failed with status code: {response.status_code}, response: {response.text}")
        except requests.exceptions.RequestException as e:
            # Catch network or request-related exceptions
            print(f"[{datetime.now()}] Heartbeat failed: {str(e)}")
        time.sleep(HEARTBEAT_INTERVAL)

# API: Store a Chunk
@app.route('/chunks/<chunk_id>', methods=['POST'])
def store_chunk(chunk_id):
    """
    Stores a received chunk in the worker's storage.
    """
    chunk_data = request.data
    if not chunk_data:
        return jsonify({'error': 'No chunk data provided'}), 400

    chunk_path = os.path.join(STORAGE_DIR, chunk_id)
    with open(chunk_path, 'wb') as chunk_file:
        chunk_file.write(chunk_data)

    return jsonify({'message': f'Chunk {chunk_id} stored successfully'}), 200

# API: Retrieve a Chunk
@app.route('/chunks/<chunk_id>', methods=['GET'])
def retrieve_chunk(chunk_id):
    """
    Retrieves a chunk by its ID.
    """
    chunk_path = os.path.join(STORAGE_DIR, chunk_id)
    if not os.path.exists(chunk_path):
        return jsonify({'error': 'Chunk not found'}), 404

    return send_file(chunk_path, as_attachment=True)

if __name__ == '__main__':
    # Start Heartbeat Thread
    threading.Thread(target=send_heartbeat, daemon=True).start()
    app.run(debug=True, port=5004)  # Run on a unique port for each worker
