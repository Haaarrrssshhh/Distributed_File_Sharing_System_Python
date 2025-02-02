from flask import Flask, request, jsonify, send_file
import os
import requests
import threading
import time
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

app = Flask(__name__)

# Worker Configuration
WORKER_ID = "worker_2"  # Change this for each worker instance
PORT = os.getenv("WORKER_2_PORT")  # Change this for each worker instance
STORAGE_DIR = os.path.abspath(f"storage/{WORKER_ID}")

# Get Worker IP - For EC2 instances, you can get the public IP dynamically
# For local testing, set WORKER_IP to '127.0.0.1'
WORKER_IP = os.getenv("WORKER_2_IP")

# Master Node Configuration
CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'config.json')
with open(CONFIG_FILE, "r") as file:
    MASTER_NODES = json.load(file)

HEARTBEAT_INTERVAL = 5  # In seconds
LEADER_CHECK_INTERVAL = 10  # In seconds
MASTER_NODE_URL = ""  # This will be dynamically updated

# Ensure storage directory exists
os.makedirs(STORAGE_DIR, exist_ok=True)

def get_current_leader():
    """
    Queries the master nodes to get the current leader's URL.
    """
    global MASTER_NODE_URL

    for master_id, master_info in MASTER_NODES.items():
        master_ip = master_info['ip']
        master_port = master_info['port']
        print(f"Querying {master_id} at {master_ip}:{master_port} for leader...")
        try:
            response = requests.get(f"http://{master_ip}:{master_port}/current_leader", timeout=2)
            if response.status_code == 200:
                leader_id = response.json().get('leader')
                if leader_id:
                    leader_info = MASTER_NODES.get(leader_id)
                    if leader_info:
                        leader_ip = leader_info['ip']
                        leader_port = leader_info['port']
                        MASTER_NODE_URL = f"http://{leader_ip}:{leader_port}"
                        print(f"Current leader is: {leader_id}")
                        return MASTER_NODE_URL
                    else:
                        print(f"Leader {leader_id} not found in MASTER_NODES")
        except requests.exceptions.RequestException as e:
            print(f"Error querying {master_id}: {e}")

    print("No leader found among master nodes.")
    return None

def send_heartbeat():
    """
    Periodically sends heartbeats to the current leader.
    """
    while True:
        leader_url = get_current_leader()
        if leader_url:
            try:
                # Send heartbeat with worker ID and URL
                heartbeat_data = {
                    'url': f"http://{WORKER_IP}:{PORT}"
                }

                response = requests.post(f"{leader_url}/heartbeat/{WORKER_ID}", json=heartbeat_data, timeout=2)
                if response.status_code == 200:
                    print(f"[{datetime.now()}] Heartbeat sent successfully to {leader_url}")
                else:
                    print(f"[{datetime.now()}] Heartbeat failed with status code: {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"Error sending heartbeat to {leader_url}: {e}")
        else:
            print("No leader found. Retrying...")

        time.sleep(HEARTBEAT_INTERVAL)


@app.route('/health',methods = ['GET'])
def health():
    return jsonify({'status': 'ok'}), 200

@app.route('/chunks/<chunk_id>', methods=['POST'])
def store_chunk(chunk_id):
    """
    Stores a received chunk in the worker's storage.
    """
    chunk_data = request.data
    if not chunk_data:
        return jsonify({'error': 'No chunk data provided'}), 400

    chunk_path = os.path.join(STORAGE_DIR, chunk_id)
    try:
        with open(chunk_path, 'wb') as chunk_file:
            chunk_file.write(chunk_data)
        print(f"Chunk {chunk_id} stored at {chunk_path}")
        return jsonify({'message': f'Chunk {chunk_id} stored successfully'}), 200
    except Exception as e:
        print(f"Error storing chunk {chunk_id}: {e}")
        return jsonify({'error': f'Failed to store chunk {chunk_id}'}), 500

@app.route('/chunks/<chunk_id>', methods=['GET'])
def retrieve_chunk(chunk_id):
    """
    Retrieves a stored chunk.
    """
    chunk_path = os.path.abspath(os.path.join(STORAGE_DIR, chunk_id))
    if not os.path.exists(chunk_path):
        print(f"Chunk {chunk_id} not found at {chunk_path}")
        return jsonify({'error': 'Chunk not found'}), 404

    try:
        return send_file(chunk_path, as_attachment=True)
    except Exception as e:
        print(f"Error retrieving chunk {chunk_id}: {e}")
        return jsonify({'error': f'Failed to retrieve chunk {chunk_id}'}), 500

@app.route('/chunks/<chunk_id>/delete', methods=['POST'])
def delete_chunk(chunk_id):
    """
    Deletes a stored chunk.
    """
    chunk_path = os.path.join(STORAGE_DIR, chunk_id)
    if os.path.exists(chunk_path):
        try:
            os.remove(chunk_path)
            print(f"Chunk {chunk_id} deleted from {chunk_path}")
            return jsonify({'message': f'Chunk {chunk_id} deleted successfully'}), 200
        except Exception as e:
            print(f"Error deleting chunk {chunk_id}: {e}")
            return jsonify({'error': f'Failed to delete chunk {chunk_id}'}), 500
    else:
        print(f"Chunk {chunk_id} not found for deletion at {chunk_path}")
        return jsonify({'error': f'Chunk {chunk_id} not found'}), 404

def leader_check():
    """
    Periodically checks for the current leader.
    """
    while True:
        get_current_leader()
        time.sleep(LEADER_CHECK_INTERVAL)

if __name__ == '__main__':
    # Start Heartbeat and Leader Check Threads
    threading.Thread(target=leader_check, daemon=True).start()
    threading.Thread(target=send_heartbeat, daemon=True).start()
    app.run(debug=True, port=PORT, host='0.0.0.0')
