from flask import Flask, request, jsonify
import sqlite3
from datetime import datetime, timedelta
import threading
import os
import json

app = Flask(__name__)

# Database file
DB_FILE = os.path.join('database', 'master_metadata.db')

# Heartbeat timeout
HEARTBEAT_TIMEOUT = 10  # In seconds

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
            INSERT INTO workers (worker_id, status, last_heartbeat)
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

if __name__ == '__main__':
    init_db()
    threading.Thread(target=monitor_workers, daemon=True).start()
    app.run(debug=True, port=5001)
