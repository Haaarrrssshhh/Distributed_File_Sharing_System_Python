from flask import Flask, request, jsonify, render_template, redirect, url_for, send_file
import os
import uuid
import requests
import hashlib
from shared.utils import log_api_call, divide_file_into_chunks
from database import db_operations
import logging

# Initialize Flask app
app = Flask(__name__)

# Configuration
WORKER_STORAGE_PATHS = {
    "worker_1": "storage/worker_1",
    "worker_2": "storage/worker_2",
    "worker_3": "storage/worker_3",
    "worker_4": "storage/worker_4",
    "worker_5": "storage/worker_5"
}
STORAGE_TEMP_PATH = "storage/temp"

MASTER_NODES = {
    "master_1": "http://10.0.2.11:5001",
    "master_2": "http://10.0.2.12:5101",
    "master_3": "http://10.0.2.13:5201"
}

# Initialize database
db_operations.init_db()

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def get_current_leader_url():
    """
    Query master nodes to discover the current leader.
    """
    for master_name, url in MASTER_NODES.items():
        try:
            logging.info(f"Querying {master_name} at {url} for leader...")
            response = requests.get(f"{url}/current_leader", timeout=2)
            if response.status_code == 200:
                leader = response.json().get("leader")
                if leader:
                    leader_url = MASTER_NODES.get(leader)
                    logging.info(f"Leader discovered: {leader} at {leader_url}")
                    return leader_url
        except requests.exceptions.RequestException as e:
            logging.error(f"Error querying {master_name}: {e}")
    raise Exception("No leader could be discovered among master nodes.")

def calculate_file_hash(file_path):
    """
    Calculate the SHA256 hash of a file.
    """
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()

@app.route('/files', methods=['POST'])
def create_file():
    """
    Upload a file, divide it into chunks, and send metadata to the master leader.
    """
    file = request.files.get('file')
    if not file:
        return jsonify({'error': 'No file provided'}), 400

    file_id = str(uuid.uuid4())
    file_name = file.filename
    file_path = os.path.join(STORAGE_TEMP_PATH, file_id)

    # Save file temporarily
    os.makedirs(STORAGE_TEMP_PATH, exist_ok=True)
    file.save(file_path)

    # Divide file into chunks
    chunks_info = divide_file_into_chunks(file_path, file_id)

    try:
        # Get leader URL and send metadata
        leader_url = get_current_leader_url()
        metadata_response = requests.post(
            f"{leader_url}/metadata",
            json={"file_id": file_id, "file_name": file_name, "chunks": chunks_info}
        )
        metadata_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to communicate with the master leader: {e}")
        os.remove(file_path)
        return jsonify({'error': 'Failed to communicate with the master leader.'}), 500

    # Store metadata in SQLite
    db_operations.add_file_record(file_id, file_name, chunks_info)
    log_api_call('CREATE', file_id, chunks_info)

    # Clean up temporary file
    os.remove(file_path)
    return jsonify({'file_id': file_id, 'chunks': chunks_info}), 201

@app.route('/files', methods=['GET'])
def get_all_files():
    """
    Get metadata for all files.
    """
    files = db_operations.get_all_files()
    log_api_call('READ', 'all_files', None)
    return jsonify(files), 200

@app.route('/files/<file_id>', methods=['GET'])
def get_file(file_id):
    """
    Get metadata for a specific file.
    """
    file_info = db_operations.get_file(file_id)
    if not file_info:
        return jsonify({'error': 'File not found'}), 404
    log_api_call('READ', file_id, file_info)
    return jsonify(file_info), 200

@app.route('/files/<file_id>/download', methods=['GET'])
def download_file(file_id):
    """
    Reconstruct a file from its chunks and serve it to the client.
    """
    file_metadata = db_operations.get_file(file_id)
    if not file_metadata:
        return jsonify({'error': 'File not found'}), 404

    output_dir = os.path.abspath(STORAGE_TEMP_PATH)
    os.makedirs(output_dir, exist_ok=True)
    output_file_path = os.path.join(output_dir, f"{file_id}_reconstructed.txt")

    try:
        leader_url = get_current_leader_url()
        with open(output_file_path, 'wb') as output_file:
            for chunk in file_metadata['chunks']:
                chunk_id = chunk['chunk_id']

                # Query master leader for worker URL
                worker_response = requests.get(f"{leader_url}/chunks/{file_id}/{chunk_id}")
                worker_response.raise_for_status()
                worker_url = worker_response.json().get('worker_url')
                if not worker_url:
                    raise Exception(f"No active worker for chunk {chunk_id}")

                # Fetch chunk data from worker
                chunk_response = requests.get(worker_url)
                chunk_response.raise_for_status()
                output_file.write(chunk_response.content)
    except Exception as e:
        logging.error(f"Failed to reconstruct file: {e}")
        return jsonify({'error': f'Failed to reconstruct file: {str(e)}'}), 500

    return send_file(output_file_path, as_attachment=True, download_name=file_metadata['name'])

@app.route('/files/<file_id>', methods=['DELETE'])
def delete_file(file_id):
    """
    Delete a file and remove its metadata and chunks.
    """
    file_info = db_operations.get_file(file_id)
    if not file_info:
        return jsonify({'error': 'File not found'}), 404

    # Mark the file as deleted in the database
    db_operations.soft_delete_file(file_id)

    try:
        leader_url = get_current_leader_url()
        requests.post(f"{leader_url}/metadata", json={"file_id": file_id, "chunks": []})
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to update metadata in the master leader: {e}")

    # Remove chunks from workers
    for chunk in file_info['chunks']:
        for worker_id in chunk['worker_ids']:
            chunk_path = os.path.join(WORKER_STORAGE_PATHS[worker_id], chunk['chunk_id'])
            if os.path.exists(chunk_path):
                os.remove(chunk_path)

    log_api_call('DELETE', file_id, file_info)
    return jsonify({'message': f'File {file_id} deleted successfully'}), 200

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5000)
