from flask import Flask, request, jsonify, send_file
import os
import requests
import threading
import time
from datetime import datetime

app = Flask(__name__)

HEARTBEAT_INTERVAL = 10  # In seconds
WORKER_ID = "worker_4"  # Change for each worker instance
STORAGE_DIR = f"distributed_file_system/storage/{WORKER_ID}"
HEARTBEAT_INTERVAL = 10  # In seconds
MASTER_NODES = ["master_1", "master_2", "master_3"]  # List of Master Nodes to query for leader information
MASTER_NODE_URL = ""  # This will be dynamically updated
LEADER_CHECK_INTERVAL = 30  # How often to check for the leader (in seconds)

# Ensure storage directory exists
os.makedirs(STORAGE_DIR, exist_ok=True)

def get_current_leader():
    """
    This function queries the Master Nodes to get the current leader's port.
    """
    global MASTER_NODE_URL

    for master in MASTER_NODES:
        try:
            # Query any master to get the current leader
            response = requests.get(f"http://127.0.0.1:{get_master_port(master)}/current_leader")
            if response.status_code == 200:
                leader = response.json().get('leader')  # This should return the leader's ID
                MASTER_NODE_URL = f"http://127.0.0.1:{get_master_port(leader)}"
                print(f"Current leader is: {leader}")
                return MASTER_NODE_URL
        except requests.exceptions.RequestException as e:
            print(f"Error querying {master}: {e}")

    return None  # If no leader is found or no master responds

def get_master_port(master):
    """
    Helper function to map Master node to a port.
    """
    ports = {
        "master_1": 5001,
        "master_2": 5101,
        "master_3": 5201
    }
    return ports.get(master, 5001)

# Heartbeat to Master Node
def send_heartbeat():
    """
    Periodically sends heartbeats to the current leader.
    """
    while True:
        # Get the current leader dynamically
        leader_url = get_current_leader()
        if leader_url:
            try:
                # Send heartbeat to the leader
                response = requests.post(f"{leader_url}/heartbeat/{WORKER_ID}")
                print(f"DEBUG: Status Code: {response.status_code}, Response Body: {response.text}")
                if response.status_code == 200:
                    print(f"[{datetime.now()}] Heartbeat sent successfully: {response.text}")
                else:
                    print(f"[{datetime.now()}] Heartbeat failed with status code: {response.status_code}, response: {response.text}")
            except requests.exceptions.RequestException as e:
                print(f"Error sending heartbeat to {leader_url}: {e}")
        else:
            print("No leader found. Retrying...")
        
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
    # Get absolute path of the chunk
    chunk_path = os.path.abspath(os.path.join(STORAGE_DIR, chunk_id))
    print(f"DEBUG: Looking for chunk at {chunk_path}")
    
    if not os.path.exists(chunk_path):
        print(f"DEBUG: Chunk {chunk_id} not found at {chunk_path}")
        return jsonify({'error': 'Chunk not found'}), 404

    try:
        return send_file(chunk_path, as_attachment=True)
    except Exception as e:
        print(f"ERROR: Failed to send chunk {chunk_id}: {e}")
        return jsonify({'error': f'Failed to retrieve chunk: {str(e)}'}), 500

if __name__ == '__main__':
    # Start Heartbeat Thread
    threading.Thread(target=send_heartbeat, daemon=True).start()
    app.run(debug=True, port=5005)  # Run on a unique port for each worker
