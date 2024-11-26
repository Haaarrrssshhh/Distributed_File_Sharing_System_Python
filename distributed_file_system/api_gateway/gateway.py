from flask import Flask, request, jsonify, render_template, redirect, url_for, send_file
import os
import requests
from datetime import datetime
import hashlib
import uuid

from database.db_operations import (
    get_active_workers,
    fetch_file_metadata,
    update_worker,
)
from database.connection import get_database

app = Flask(__name__)

# Initialize the database
db = get_database()

# Master Node URLs
MASTER_NODES = {
    "master_1": 5001,
    "master_2": 5101,
    "master_3": 5201,
}

def get_current_leader_url():
    for master, port in MASTER_NODES.items():
        try:
            print(f"Querying {master} at port {port} for leader...")
            response = requests.get(f"http://127.0.0.1:{port}/current_leader", timeout=2)
            if response.status_code == 200:
                leader = response.json().get("leader")
                print(f"Leader discovered from {master}: {leader}")
                if leader:
                    leader_port = MASTER_NODES.get(leader)
                    if leader_port:
                        print(f"Returning leader URL: http://127.0.0.1:{leader_port}")
                        return f"http://127.0.0.1:{leader_port}"
        except requests.exceptions.RequestException as e:
            print(f"Error querying {master}: {e}")

    raise Exception("No leader could be discovered among master nodes.")

def calculate_file_hash(file_data):
    """
    Calculate the SHA256 hash of file data.

    Args:
        file_data (bytes): File data in bytes.

    Returns:
        str: SHA256 hash of the file data.
    """
    hash_sha256 = hashlib.sha256()
    hash_sha256.update(file_data)
    return hash_sha256.hexdigest()

@app.route('/files', methods=['POST'])
def create_file():
    """
    Upload file to leader master node.
    """
    file = request.files.get('file')
    if not file:
        return jsonify({'error': 'No file provided'}), 400

    file_name = file.filename
    file_data = file.read()

    try:
        leader_url = get_current_leader_url()  # Identify the leader master node
        response = requests.post(
            f"{leader_url}/upload_file",
            files={'file': (file_name, file_data)}
        )
        return response.json(), response.status_code
    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Master node communication failed: {str(e)}'}), 500

@app.route('/files/<file_id>/delete', methods=['POST'])
def delete_file_post(file_id):
    """
    Soft delete a file by notifying the leader master node.
    """
    try:
        leader_url = get_current_leader_url()
        response = requests.delete(f"{leader_url}/files/{file_id}")
        if response.status_code == 200:
            return redirect(url_for('index'))
        else:
            return jsonify({'error': response.json().get('error', 'Unknown error')}), response.status_code
    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Master node communication failed: {str(e)}'}), 500

@app.route('/files/<file_id>/download', methods=['GET'])
def download_file(file_id):
    """
    Download a file by reconstructing it from its chunks.
    """
    file_metadata = fetch_file_metadata(file_id)
    if not file_metadata:
        return jsonify({'error': 'File not found'}), 404

    output_dir = os.path.abspath(os.path.join('storage', 'temp'))
    os.makedirs(output_dir, exist_ok=True)
    output_file_path = os.path.join(output_dir, f"{file_metadata['file_name']}")

    try:
        leader_url = get_current_leader_url()
        with open(output_file_path, 'wb') as output_file:
            for chunk in file_metadata['chunks']:
                chunk_id = chunk['chunk_id']
                worker_ids = chunk['worker_ids']
                chunk_retrieved = False

                # Try to retrieve the chunk from the assigned workers
                for worker_id in worker_ids:
                    # Fetch worker URL from active workers
                    active_workers = get_active_workers()
                    worker_info = next((w for w in active_workers if w['worker_id'] == worker_id), None)
                    if worker_info:
                        worker_url = worker_info['url']
                        try:
                            chunk_response = requests.get(f"{worker_url}/chunks/{chunk_id}")
                            chunk_response.raise_for_status()
                            output_file.write(chunk_response.content)
                            chunk_retrieved = True
                            break  # Break if chunk is retrieved successfully
                        except requests.exceptions.RequestException as e:
                            print(f"Failed to retrieve chunk {chunk_id} from worker {worker_id}: {e}")
                    else:
                        print(f"Worker {worker_id} is not active.")

                if not chunk_retrieved:
                    return jsonify({'error': f'Failed to retrieve chunk {chunk_id} from any worker'}), 500

        return send_file(output_file_path, as_attachment=True, download_name=file_metadata['file_name'])
    except Exception as e:
        return jsonify({'error': f'Failed to reconstruct file: {str(e)}'}), 500

@app.route('/')
def index():
    """
    Render the index page with a list of all files.
    """
    files_collection = db["files"]
    files_cursor = files_collection.find()
    files = []
    for file_doc in files_cursor:
        file = {
            'file_id': file_doc.get('file_id'),
            'file_name': file_doc.get('file_name'),
            'created_at': file_doc.get('created_at').strftime("%Y-%m-%d %H:%M:%S") if file_doc.get('created_at') else '',
            'chunks': file_doc.get('chunks', []),
            'status': file_doc.get('status', 'active'),  # Default to 'active' if not set
            'deleted_at': file_doc.get('deleted_at')
        }
        files.append(file)
    return render_template('index.html', files=files)


# Worker Heartbeat API: Relay worker heartbeats to Master Node
@app.route('/heartbeat/<worker_id>', methods=['POST'])
def worker_heartbeat(worker_id):
    worker_url = request.json.get('url')  # Expect worker to send its full URL

    if not worker_url:
        return jsonify({'error': 'Worker URL not provided'}), 400

    # Update or insert worker info
    update_worker(worker_id, worker_url)
    return jsonify({'message': f'Heartbeat received from {worker_id}'}), 200

if __name__ == '__main__':
    app.run(debug=True, port=5000)
