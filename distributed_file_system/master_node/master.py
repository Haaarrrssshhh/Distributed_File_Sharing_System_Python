from flask import Flask, request, jsonify
import sqlite3
from datetime import datetime, timedelta
import threading
import os
import json
import time
import requests
import sys
from threading import Lock
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
DB_FILE = f"database/{MASTER_NODE_ID}_metadata.db"
BACKUP_MASTERS = list(config.keys())  # All masters from config
HEARTBEAT_INTERVAL = 10  # Seconds to check if current leader is alive
HEARTBEAT_TIMEOUT = 10  # In seconds
current_leader = None  # Track the current leader dynamically

# Initialize the database
def init_db():
    if not os.path.exists(os.path.dirname(DB_FILE)):
        os.makedirs(os.path.dirname(DB_FILE))

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Metadata for files and chunks
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS metadata (
            file_id TEXT PRIMARY KEY,
            file_name TEXT NOT NULL,
            chunks TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT
        )
    ''')

    # Worker node status
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS workers (
            worker_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            last_heartbeat TEXT NOT NULL
        )
    ''')

    conn.commit()
    conn.close()

# Election logic: Bully Algorithm
def start_election():
    global current_leader
    candidate_id = MASTER_NODE_ID
    higher_nodes = [node for node in BACKUP_MASTERS if node > candidate_id]
    higher_nodes_alive = []

    print(f"{candidate_id} starting election.")

    # Send election message to all higher-priority nodes
    for node in higher_nodes:
        try:
            response = requests.get(f"http://127.0.0.1:{get_leader_port(node)}/alive", timeout=2)
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

@app.route('/alive', methods=['GET'])
def alive():
    """
    Respond to alive checks during the Bully Algorithm election.
    """
    return jsonify({'status': 'alive'}), 200

# Heartbeat checking: Checks if the current leader is alive
def check_leader_status():
    """
    This function periodically checks if the leader is alive.
    If no leader is found, it tries to discover one before starting an election.
    """
    global current_leader

    while True:
        if current_leader is None:
            print(f"{MASTER_NODE_ID}: No leader found. Trying to discover leader...")
            discover_leader()
            if current_leader is None:
                print(f"{MASTER_NODE_ID}: Starting election process...")
                start_election()
        else:
            # Check if the current leader is alive
            if current_leader == MASTER_NODE_ID:
                # I am the leader
                print(f"{MASTER_NODE_ID}: I am the leader.")
            else:
                # Check if the leader is alive
                try:
                    response = requests.get(f"http://127.0.0.1:{get_leader_port(current_leader)}/alive", timeout=2)
                    if response.status_code == 200:
                        print(f"{MASTER_NODE_ID}: Leader {current_leader} is alive.")
                    else:
                        print(f"{MASTER_NODE_ID}: Leader {current_leader} failed. Starting election...")
                        current_leader = None
                except requests.exceptions.RequestException:
                    print(f"{MASTER_NODE_ID}: Leader {current_leader} unreachable. Starting election...")
                    current_leader = None

        # Sleep before checking again
        time.sleep(HEARTBEAT_INTERVAL)

def discover_leader():
    """
    Attempts to discover the current leader by querying other nodes.
    """
    global current_leader
    for node in BACKUP_MASTERS:
        if node != MASTER_NODE_ID:
            try:
                response = requests.get(f"http://127.0.0.1:{get_leader_port(node)}/current_leader", timeout=2)
                if response.status_code == 200:
                    leader = response.json().get('leader')
                    if leader:
                        current_leader = leader
                        print(f"{MASTER_NODE_ID}: Discovered leader {current_leader} from {node}")
                        synchronize_metadata_with_leader()
                        return
            except requests.exceptions.RequestException:
                continue
    # If no leader is found after querying all nodes
    current_leader = None

def synchronize_metadata_with_leader():
    """
    Synchronize metadata from the current leader.
    """
    if current_leader and current_leader != MASTER_NODE_ID:
        leader_port = get_leader_port(current_leader)
        try:
            response = requests.get(f"http://127.0.0.1:{leader_port}/all_metadata", timeout=5)
            if response.status_code == 200:
                metadata_list = response.json().get('metadata', [])
                conn = sqlite3.connect(DB_FILE)
                cursor = conn.cursor()
                for metadata in metadata_list:
                    file_id = metadata['file_id']
                    file_name = metadata['file_name']
                    chunks = json.dumps(metadata['chunks'])  # Convert list to JSON
                    created_at = metadata['created_at']
                    updated_at = metadata.get('updated_at')
                    cursor.execute('''
                        INSERT OR REPLACE INTO metadata (file_id, file_name, chunks, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (file_id, file_name, chunks, created_at, updated_at))
                conn.commit()
                conn.close()
                print(f"{MASTER_NODE_ID}: Synchronized metadata with leader {current_leader}")
            else:
                print(f"{MASTER_NODE_ID}: Failed to synchronize metadata with leader {current_leader}")
        except requests.exceptions.RequestException as e:
            print(f"{MASTER_NODE_ID}: Error synchronizing metadata with leader: {e}")


def get_leader_port(leader):
    """
    Helper function to map leader node to a port.
    """
    return config.get(leader, {}).get("port", 5001)  # Default to 5001 if leader is not found

@app.route('/workers/active', methods=['GET'])
def get_active_workers():
    """
    Retrieve the list of active workers based on their last heartbeat.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT worker_id FROM workers
        WHERE status = 'active'
    ''')
    rows = cursor.fetchall()
    conn.close()

    return jsonify({'active_workers': [row[0] for row in rows]}), 200

@app.route('/chunks/<file_id>/<chunk_id>', methods=['GET'])
def get_chunk_worker(file_id, chunk_id):
    """
    Return the worker URL storing a specific chunk.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Fetch file metadata
    cursor.execute('SELECT chunks FROM metadata WHERE file_id = ?', (file_id,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        return jsonify({'error': 'File not found'}), 404

    chunks = json.loads(row[0])
    for chunk in chunks:
        if chunk['chunk_id'] == chunk_id:
            for worker_id in chunk['worker_ids']:
                # Check if the worker is active
                conn = sqlite3.connect(DB_FILE)
                cursor = conn.cursor()
                cursor.execute('SELECT status FROM workers WHERE worker_id = ?', (worker_id,))
                worker_row = cursor.fetchone()
                conn.close()

                if worker_row and worker_row[0] == 'active':
                    port = {
                        "worker_1": 5002,
                        "worker_2": 5003,
                        "worker_3": 5004,
                        "worker_4": 5005,
                        "worker_5": 5006,
                    }.get(worker_id)
                    if port:
                        return jsonify({'worker_url': f"http://127.0.0.1:{port}/chunks/{chunk_id}"}), 200

    return jsonify({'error': 'No active workers for chunk'}), 500

@app.route('/metadata', methods=['POST'])
def update_metadata():
    data = request.json
    file_id = data.get('file_id')
    file_name = data.get('file_name')
    chunks = json.dumps(data.get('chunks'))  # Convert list to JSON
    created_at = datetime.now().isoformat()

    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO metadata (file_id, file_name, chunks, created_at)
            VALUES (?, ?, ?, ?)
        ''', (file_id, file_name, chunks, created_at))
        conn.commit()
        conn.close()
        sync_metadata_across_masters(data)
        print(f"DEBUG: Metadata for file {file_id} updated successfully.")
        return jsonify({'message': f'Metadata for file {file_id} updated successfully'}), 200
    except Exception as e:
        print(f"ERROR: Failed to update metadata: {e}")
        return jsonify({'error': 'Failed to update metadata'}), 500

@app.route('/heartbeat/<worker_id>', methods=['POST'])
def worker_heartbeat(worker_id):
    """
    Update the heartbeat status of a worker node.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Check if the worker already exists
    cursor.execute('SELECT worker_id FROM workers WHERE worker_id = ?', (worker_id,))
    if cursor.fetchone():
        # Update the last heartbeat time
        cursor.execute('''
            UPDATE workers
            SET status = 'active', last_heartbeat = ?
            WHERE worker_id = ?
        ''', (datetime.now().isoformat(), worker_id))
    else:
        # Insert new worker with active status
        cursor.execute('''
            INSERT OR REPLACE INTO workers (worker_id, status, last_heartbeat)
            VALUES (?, 'active', ?)
        ''', (worker_id, datetime.now().isoformat()))

    conn.commit()
    conn.close()

    return jsonify({'message': f'Heartbeat received from {worker_id}'}), 200

def monitor_workers():
    """
    Monitor worker nodes' heartbeat and update their status.
    """
    while True:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # Get all workers and their last heartbeat
        cursor.execute('SELECT worker_id, last_heartbeat FROM workers')
        rows = cursor.fetchall()

        for worker_id, last_heartbeat in rows:
            last_heartbeat_time = datetime.fromisoformat(last_heartbeat)
            if (datetime.now() - last_heartbeat_time) > timedelta(seconds=HEARTBEAT_TIMEOUT):
                # Mark worker as inactive
                cursor.execute('''
                    UPDATE workers
                    SET status = 'inactive'
                    WHERE worker_id = ?
                ''', (worker_id,))

        conn.commit()
        conn.close()
        threading.Event().wait(HEARTBEAT_TIMEOUT)

@app.route('/current_leader', methods=['GET'])
def current_leader_endpoint():
    """
    Return the current leader's ID.
    """
    global current_leader
    return jsonify({'leader': current_leader}), 200

def announce_leader():
    global current_leader
    for node in BACKUP_MASTERS:
        if node != MASTER_NODE_ID:
            try:
                requests.post(f"http://127.0.0.1:{get_leader_port(node)}/leader", json={'leader': current_leader}, timeout=2)
            except requests.exceptions.RequestException as e:
                print(f"{MASTER_NODE_ID}: Failed to announce leader to {node}: {e}")

@app.route('/leader', methods=['POST'])
def leader_announcement():
    global current_leader
    data = request.get_json()
    leader = data.get('leader')
    current_leader = leader
    print(f"{MASTER_NODE_ID}: New leader announced: {current_leader}")
    return jsonify({'status': 'ok'}), 200

@app.route('/sync_metadata', methods=['POST'])
def sync_metadata():
    """
    Synchronizes metadata sent from another master node.
    """
    data = request.json
    file_id = data.get('file_id')
    file_name = data.get('file_name')
    chunks = json.dumps(data.get('chunks'))  # Convert list to JSON
    created_at = datetime.now().isoformat()

    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO metadata (file_id, file_name, chunks, created_at)
            VALUES (?, ?, ?, ?)
        ''', (file_id, file_name, chunks, created_at))
        conn.commit()
        conn.close()
        print(f"DEBUG: Synced metadata for file {file_id} successfully.")
        return jsonify({'message': f'Metadata for file {file_id} synced successfully'}), 200
    except Exception as e:
        print(f"ERROR: Failed to sync metadata: {e}")
        return jsonify({'error': 'Failed to sync metadata'}), 500
        
@app.route('/all_metadata', methods=['GET'])
def get_all_metadata():
    """
    Return all metadata entries, including soft-deleted files.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT file_id, file_name, chunks, created_at, updated_at, deleted_at
        FROM metadata
    ''')
    rows = cursor.fetchall()
    conn.close()

    metadata = []
    for row in rows:
        metadata.append({
            'file_id': row[0],
            'file_name': row[1],
            'chunks': json.loads(row[2]),
            'created_at': row[3],
            'updated_at': row[4],
            'deleted_at': row[5]
        })
    return jsonify({'metadata': metadata}), 200

failed_syncs_lock = Lock()
failed_syncs = {}

def sync_metadata_across_masters(metadata):
    global failed_syncs
    for master in BACKUP_MASTERS:
        if master != MASTER_NODE_ID:
            try:
                response = requests.post(
                    f"http://127.0.0.1:{get_leader_port(master)}/sync_metadata",
                    json=metadata,
                    timeout=5  # Adjust timeout if needed
                )
                if response.status_code == 200:
                    print(f"Metadata synced with {master}")
                    with failed_syncs_lock:
                        if master in failed_syncs and metadata in failed_syncs[master]:
                            failed_syncs[master].remove(metadata)
                            if not failed_syncs[master]:
                                del failed_syncs[master]
                else:
                    print(f"Failed to sync metadata with {master}: {response.status_code}")
                    with failed_syncs_lock:
                        failed_syncs.setdefault(master, []).append(metadata)
            except requests.exceptions.RequestException as e:
                print(f"Error syncing metadata with {master}: {e}")
                with failed_syncs_lock:
                    failed_syncs.setdefault(master, []).append(metadata)

def retry_failed_syncs():
    """
    Retries metadata synchronization for failed nodes periodically.
    """
    while True:
        with failed_syncs_lock:
            masters = list(failed_syncs.keys())
        for master in masters:
            with failed_syncs_lock:
                metadata_list = failed_syncs.get(master, []).copy()
            for metadata in metadata_list:
                try:
                    response = requests.post(
                        f"http://127.0.0.1:{get_leader_port(master)}/sync_metadata",
                        json=metadata,
                        timeout=5
                    )
                    if response.status_code == 200:
                        print(f"Retried metadata sync with {master} successfully.")
                        with failed_syncs_lock:
                            failed_syncs[master].remove(metadata)
                            if not failed_syncs[master]:
                                del failed_syncs[master]
                    else:
                        print(f"Retry failed for {master}: {response.status_code}")
                except requests.exceptions.RequestException as e:
                    print(f"Retry failed for {master}: {e}")
        time.sleep(30)  # Retry every 30 seconds

if __name__ == '__main__':
    init_db()
    discover_leader()  # Discover leader and synchronize metadata on startup
    heartbeat_thread = threading.Thread(target=check_leader_status, daemon=True)
    heartbeat_thread.start()  # Start heartbeat thread
    worker_monitor_thread = threading.Thread(target=monitor_workers, daemon=True)
    worker_monitor_thread.start()  # Start worker monitoring thread
    retry_thread = threading.Thread(target=retry_failed_syncs, daemon=True)
    retry_thread.start()
    app.run(debug=True, port=PORT, use_reloader=False)