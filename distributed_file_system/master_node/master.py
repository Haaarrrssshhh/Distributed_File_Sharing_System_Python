from flask import Flask, request, jsonify
from datetime import datetime
import threading
import json
import uuid
import time
import requests
import sys
import random

from database.connection import get_database
from database.db_operations import (
    get_metadata_collection,
    mark_inactive_workers,
    store_file_metadata,
    fetch_file_metadata,
    update_leader_metadata,
    fetch_leader_metadata,
    get_active_workers,
    update_worker
)

app = Flask(__name__)

# Load Configuration from `config.json`
with open("config.json", "r") as config_file:
    config = json.load(config_file)

# Get Master Node ID from Command-Line Argument
if len(sys.argv) != 2:
    print("Usage: python master.py <master_node_id>")
    sys.exit(1)

MASTER_NODE_ID = sys.argv[1]  # E.g., "master_1", "master_2", "master_3"

if MASTER_NODE_ID not in config:
    print(f"Error: {MASTER_NODE_ID} not found in config.json")
    sys.exit(1)

PORT = config[MASTER_NODE_ID]["port"]
BACKUP_MASTERS = [node for node in config.keys() if node != MASTER_NODE_ID]  # Exclude self
HEARTBEAT_INTERVAL = 5  # Seconds to check if current leader is alive
LEADER_CHECK_INTERVAL = 10  # How often to sync the leader from MongoDB
HEARTBEAT_TIMEOUT = 15  # Workers are considered inactive if no heartbeat for 15 seconds
WORKER_CHECK_INTERVAL = 5  # How often to check for inactive workers
current_leader = None  # Track the current leader dynamically

db = get_database()

def announce_leader():
    """
    Notify other master nodes about the newly elected leader.
    Write the leader to MongoDB.
    """
    global current_leader

    # Persist leader information in MongoDB
    update_leader_metadata(current_leader)

    # Notify all other masters
    for node in BACKUP_MASTERS:
        node_port = config[node]["port"]
        try:
            response = requests.post(
                f"http://127.0.0.1:{node_port}/leader",
                json={'leader': current_leader},
                timeout=2
            )
            if response.status_code == 200:
                print(f"{MASTER_NODE_ID}: Successfully announced leader to {node}")
            else:
                print(f"{MASTER_NODE_ID}: Failed to announce leader to {node}")
        except requests.exceptions.RequestException as e:
            print(f"{MASTER_NODE_ID}: Error announcing leader to {node}: {e}")

@app.route('/leader', methods=['POST'])
def leader_announcement():
    """
    Handle leader announcements from other masters.
    """
    global current_leader
    data = request.get_json()
    leader = data.get('leader')

    # Update the leader in the local state
    current_leader = leader
    print(f"{MASTER_NODE_ID}: New leader acknowledged: {current_leader}")
    return jsonify({'status': 'ok'}), 200

def start_election():
    """
    Bully Algorithm for leader election.
    """
    global current_leader
    candidate_id = MASTER_NODE_ID
    higher_nodes = [node for node in config.keys() if node > candidate_id]

    print(f"{candidate_id} starting election.")

    # Send election message to all higher-priority nodes
    higher_nodes_alive = []
    for node in higher_nodes:
        node_port = config[node]["port"]
        try:
            response = requests.get(f"http://127.0.0.1:{node_port}/alive", timeout=2)
            if response.status_code == 200:
                higher_nodes_alive.append(node)
        except requests.exceptions.RequestException:
            # Node is not reachable
            continue

    if higher_nodes_alive:
        # Higher-priority node(s) are alive, so we wait for them to declare leader
        print(f"{candidate_id}: Higher-priority node(s) {higher_nodes_alive} are alive. Waiting for them to declare leader.")
    else:
        # No higher-priority nodes are alive, so declare self as leader
        current_leader = candidate_id
        print(f"{candidate_id}: New Leader elected: {current_leader}")
        announce_leader()

def get_leader_from_mongo():
    """
    Fetch the current leader from MongoDB.
    """
    leader_data = fetch_leader_metadata()
    if leader_data:
        return leader_data.get("leader")
    return None

def discover_leader():
    """
    Attempts to discover the current leader by querying MongoDB.
    Verifies if the leader is alive.
    """
    global current_leader
    leader = get_leader_from_mongo()
    if leader:
        if is_leader_alive(leader):
            current_leader = leader
            print(f"{MASTER_NODE_ID}: Discovered leader {current_leader} from MongoDB")
        else:
            print(f"{MASTER_NODE_ID}: Leader {leader} from MongoDB is not alive.")
            start_election()
    else:
        print(f"{MASTER_NODE_ID}: No leader found in MongoDB.")
        start_election()

def get_leader_port(leader):
    """
    Helper function to map leader node to a port.
    """
    return config.get(leader, {}).get("port", 5001)  # Default to 5001 if leader is not found

@app.route('/current_leader', methods=['GET'])
def current_leader_endpoint():
    """
    Return the current leader's ID.
    """
    global current_leader
    return jsonify({'leader': current_leader}), 200

def is_leader_alive(leader_id):
    """
    Check if the leader is alive.
    """
    if leader_id == MASTER_NODE_ID:
        return True  # We are the leader
    leader_port = get_leader_port(leader_id)
    try:
        response = requests.get(f"http://127.0.0.1:{leader_port}/alive", timeout=2)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def check_leader_alive():
    """
    Periodically check if the current leader is alive.
    If not, start a new election.
    """
    global current_leader
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        if current_leader != MASTER_NODE_ID:
            if not is_leader_alive(current_leader):
                print(f"{MASTER_NODE_ID}: Leader {current_leader} is unresponsive. Starting election.")
                start_election()
        else:
            # If we are the leader, no need to check
            pass

@app.route('/alive', methods=['GET'])
def alive():
    """
    Respond to alive checks during the Bully Algorithm election.
    """
    return jsonify({'status': 'alive'}), 200

@app.route('/upload_file', methods=['POST'])
def upload_file():
    """
    Handle file uploads from the gateway.
    """
    if current_leader != MASTER_NODE_ID:
        return jsonify({'error': 'This node is not the leader'}), 403

    file = request.files.get('file')
    if not file:
        return jsonify({'error': 'No file provided'}), 400

    file_name = file.filename
    file_data = file.read()
    file_size = len(file_data)
    file_id = str(uuid.uuid4())
    print(file_id,"file_isssssd")
    # Divide file into chunks and assign to workers
    try:
        chunks_info = divide_file_into_chunks(file_data, file_id)
        # Store file metadata
        store_file_metadata(
            file_id=file_id,
            file_name=file_name,
            size=file_size,
            chunks=chunks_info
        )
        return jsonify({'message': f'File {file_name} uploaded successfully', 'file_id': file_id}), 200
    except Exception as e:
        return jsonify({'error': f'Failed to upload file: {str(e)}'}), 500

def divide_file_into_chunks(file_data, file_id, chunk_size_mb=4, replication_factor=3):
    """
    Divide the file data into chunks and assign to active workers.
    """
    chunk_size = chunk_size_mb * 1024 * 1024
    file_size = len(file_data)
    num_chunks = (file_size + chunk_size - 1) // chunk_size
    chunks_info = []

    # Fetch active workers
    active_workers_info = get_active_workers()
    active_workers = [worker['worker_id'] for worker in active_workers_info]
    worker_urls = {worker['worker_id']: worker['url'] for worker in active_workers_info}

    if len(active_workers) < replication_factor:
        raise Exception("Not enough active workers to replicate chunks")

    for i in range(num_chunks):
        start_index = i * chunk_size
        end_index = min(start_index + chunk_size, file_size)
        chunk_data = file_data[start_index:end_index]
        chunk_id = f"{file_id}_chunk_{i+1}"
        assigned_workers = random.sample(active_workers, replication_factor)

        for worker_id in assigned_workers:
            worker_url = worker_urls[worker_id]
            try:
                chunk_response = requests.post(f"{worker_url}/chunks/{chunk_id}", data=chunk_data)
                chunk_response.raise_for_status()
            except requests.exceptions.RequestException as e:
                raise Exception(f"Failed to store chunk {chunk_id} on worker {worker_id}: {e}")

        chunks_info.append({'chunk_id': chunk_id, 'size': len(chunk_data), 'worker_ids': assigned_workers})

    return chunks_info

@app.route('/files/<file_id>', methods=['DELETE'])
def delete_file(file_id):
    """
    Handle file deletion requests.
    """
    if current_leader != MASTER_NODE_ID:
        return jsonify({'error': 'This node is not the leader'}), 403

    file_metadata = fetch_file_metadata(file_id)
    if not file_metadata:
        return jsonify({'error': 'File not found'}), 404

    # Delete chunks from workers
    for chunk in file_metadata['chunks']:
        chunk_id = chunk['chunk_id']
        worker_ids = chunk['worker_ids']

        for worker_id in worker_ids:
            worker_info = next((w for w in get_active_workers() if w['worker_id'] == worker_id), None)
            if worker_info:
                worker_url = worker_info['url']
                try:
                    response = requests.post(f"{worker_url}/chunks/{chunk_id}/delete")
                    response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    print(f"Failed to delete chunk {chunk_id} from worker {worker_id}: {e}")
            else:
                print(f"Worker {worker_id} is not active.")

    # Remove file metadata
    files_collection = db["files"]
    files_collection.update_one(
        {"file_id": file_id},
        {"$set": {"status": "deleted", "deleted_at": datetime.utcnow()}}
    )

    return jsonify({'message': f'File {file_id} deleted successfully'}), 200

@app.route('/chunks/<file_id>/<chunk_id>', methods=['GET'])
def get_chunk_worker_url(file_id, chunk_id):
    """
    Return the URL of a worker that has the requested chunk.
    """
    file_metadata = fetch_file_metadata(file_id)
    if not file_metadata:
        return jsonify({'error': 'File not found'}), 404

    chunk = next((c for c in file_metadata['chunks'] if c['chunk_id'] == chunk_id), None)
    if not chunk:
        return jsonify({'error': 'Chunk not found'}), 404

    worker_ids = chunk['worker_ids']
    active_workers_info = get_active_workers()
    active_workers = {worker['worker_id']: worker['url'] for worker in active_workers_info}

    for worker_id in worker_ids:
        if worker_id in active_workers:
            worker_url = active_workers[worker_id]
            return jsonify({'worker_url': f"{worker_url}/chunks/{chunk_id}"}), 200

    return jsonify({'error': 'No active worker has this chunk'}), 500

@app.route('/heartbeat/<worker_id>', methods=['POST'])
def worker_heartbeat(worker_id):
    """
    Handle heartbeats from workers.
    """
    worker_url = request.json.get('url')  # Expect worker to send its full URL

    if not worker_url:
        return jsonify({'error': 'Worker URL not provided'}), 400

    # Update or insert worker info
    update_worker(worker_id, worker_url)
    return jsonify({'message': f'Heartbeat received from {worker_id}'}), 200


def check_inactive_workers():
    """
    Periodically checks for inactive workers and updates their status.
    """
    while True:
        inactive_count = mark_inactive_workers(HEARTBEAT_TIMEOUT)
        if inactive_count > 0:
            print(f"{MASTER_NODE_ID}: Marked {inactive_count} worker(s) as inactive.")
        time.sleep(WORKER_CHECK_INTERVAL)

if __name__ == '__main__':
    discover_leader()  # Discover leader and synchronize metadata on startup
    threading.Thread(target=check_leader_alive, daemon=True).start()  # Check leader periodically
    threading.Thread(target=check_inactive_workers, daemon=True).start()  # Check workers periodically
    app.run(debug=True, port=PORT, use_reloader=False)
