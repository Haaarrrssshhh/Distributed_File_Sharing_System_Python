from flask import Flask, request, jsonify
import sqlite3
from datetime import datetime, timedelta
import threading
import os
import json
import time
import requests
import sys

app = Flask(__name__)

# Database file
DB_FILE = os.path.join('database', 'master_metadata.db')

# Role assignment: default is primary
ROLE = sys.argv[1] if len(sys.argv) > 1 else "primary"

# Heartbeat and metadata sync configurations
HEARTBEAT_TIMEOUT = 10  # Timeout in seconds
HEARTBEAT_INTERVAL = 5  # Interval to send/monitor heartbeats
BACKUP_NODE = {"host": "127.0.0.1", "port": 5002}  # Hardcoded for simplicity
last_heartbeat_time = 0

# Initialize the database
def init_db():
    if not os.path.exists('database'):
        os.makedirs('database')

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

@app.route('/metadata', methods=['POST'])
def update_metadata():
    """
    Update metadata when a new file is created or modified.
    """
    data = request.json
    file_id = data.get('file_id')
    file_name = data.get('file_name')
    chunks = json.dumps(data.get('chunks'))  # Convert list to JSON
    created_at = datetime.now().isoformat()

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('''
        INSERT OR REPLACE INTO metadata (file_id, file_name, chunks, created_at)
        VALUES (?, ?, ?, ?)
    ''', (file_id, file_name, chunks, created_at))

    conn.commit()
    conn.close()

    return jsonify({'message': f'Metadata for file {file_id} updated successfully'}), 200

@app.route('/metadata/<file_id>', methods=['GET'])
def get_metadata(file_id):
    """
    Retrieve metadata for a specific file by file ID.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT file_id, file_name, chunks, created_at, updated_at
        FROM metadata
        WHERE file_id = ?
    ''', (file_id,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        return jsonify({'error': 'File not found'}), 404

    return jsonify({
        'file_id': row[0],
        'file_name': row[1],
        'chunks': json.loads(row[2]),
        'created_at': row[3],
        'updated_at': row[4],
    }), 200

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
            INSERT INTO workers (worker_id, status, last_heartbeat)
            VALUES (?, 'active', ?)
        ''', (worker_id, datetime.now().isoformat()))

    conn.commit()
    conn.close()

    return jsonify({'message': f'Heartbeat received from {worker_id}'}), 200

@app.route('/workers', methods=['GET'])
def get_workers():
    """
    Retrieve the status of all worker nodes.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('SELECT worker_id, status, last_heartbeat FROM workers')
    rows = cursor.fetchall()
    conn.close()

    workers = [
        {'worker_id': row[0], 'status': row[1], 'last_heartbeat': row[2]}
        for row in rows
    ]

    return jsonify(workers), 200

@app.route('/heartbeat', methods=['POST'])
def receive_heartbeat():
    """
    Receive and process heartbeat messages from the primary.
    """
    global last_heartbeat_time
    data = request.json
    if data.get("leader") == "primary":
        last_heartbeat_time = time.time()
        print("Heartbeat received from primary")
        return jsonify({"message": "Heartbeat received"}), 200
    return jsonify({"error": "Invalid leader"}), 400

@app.route('/sync_metadata', methods=['POST'])
def sync_metadata():
    """
    Receive and update metadata from the primary.
    """
    data = request.json
    metadata = data.get("metadata", [])
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # Clear existing metadata and replace with synced data
        cursor.execute("DELETE FROM metadata")
        for row in metadata:
            cursor.execute(
                "INSERT INTO metadata (file_id, file_name, chunks, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                row
            )
        conn.commit()
        conn.close()

        print("Metadata synchronized successfully")
        return jsonify({"message": "Metadata synchronized"}), 200
    except Exception as e:
        print(f"Metadata sync error: {e}")
        return jsonify({"error": "Failed to sync metadata"}), 500

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

def monitor_heartbeat():
    """
    Monitor the primary's heartbeat and trigger leader election if timeout occurs.
    """
    global last_heartbeat_time
    while ROLE == "backup":
        if time.time() - last_heartbeat_time > HEARTBEAT_TIMEOUT:
            print("Primary node not responding. Starting leader election...")
            start_leader_election()
        time.sleep(1)

def send_heartbeat_to_backup():
    """
    Send periodic heartbeats to the backup node.
    """
    while ROLE == "primary":
        try:
            response = requests.post(
                f"http://{BACKUP_NODE['host']}:{BACKUP_NODE['port']}/heartbeat",
                json={"leader": "primary"}
            )
            print("Heartbeat sent to backup" if response.status_code == 200 else "Failed to send heartbeat")
        except Exception as e:
            print(f"Failed to send heartbeat: {e}")
        time.sleep(HEARTBEAT_INTERVAL)

def synchronize_metadata():
    """
    Send metadata updates to the backup node periodically.
    """
    while ROLE == "primary":
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM metadata")
            metadata = cursor.fetchall()
            conn.close()

            response = requests.post(
                f"http://{BACKUP_NODE['host']}:{BACKUP_NODE['port']}/sync_metadata",
                json={"metadata": metadata}
            )
            if response.status_code == 200:
                print("Metadata synchronized with backup")
            else:
                print("Metadata sync failed")
        except Exception as e:
            print(f"Metadata sync error: {e}")
        time.sleep(HEARTBEAT_INTERVAL)

def start_leader_election():
    """
    Promote backup to primary during leader election.
    """
    global ROLE
    ROLE = "primary"
    print("Backup node is now the primary leader.")

if __name__ == '__main__':
    init_db()
    # Start worker monitoring in a separate thread
    threading.Thread(target=monitor_workers, daemon=True).start()

    if ROLE == "primary":
        threading.Thread(target=send_heartbeat_to_backup, daemon=True).start()
        threading.Thread(target=synchronize_metadata, daemon=True).start()
    elif ROLE == "backup":
        threading.Thread(target=monitor_heartbeat, daemon=True).start()

    app.run(debug=True, port=(5001 if ROLE == "primary" else 5002))
