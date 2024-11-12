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
    # Start worker monitoring in a separate thread
    threading.Thread(target=monitor_workers, daemon=True).start()
    app.run(debug=True, port=5001)
