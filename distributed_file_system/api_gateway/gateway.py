from flask import Flask, request, jsonify, render_template, redirect, url_for, send_file
import os
import uuid
import requests
from shared.utils import log_api_call, divide_file_into_chunks
from database import db_operations
import hashlib

app = Flask(__name__)

# Master Node URL (update if needed)
MASTER_NODE_URL = "http://127.0.0.1:5001"
WORKER_STORAGE_PATHS = {
    "worker_1": "storage/worker_1",
    "worker_2": "storage/worker_2",
    "worker_3": "storage/worker_3",
    "worker_4": "storage/worker_4",
    "worker_5": "storage/worker_5"
}

# Initialize the SQLite database
db_operations.init_db()

def calculate_file_hash(file_path):
    """
    Calculate the SHA256 hash of a file.

    Args:
        file_path (str): Path to the file.

    Returns:
        str: SHA256 hash of the file.
    """
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()

# Create API: Upload a file and divide it into chunks
@app.route('/files', methods=['POST'])
def create_file():
    file = request.files.get('file')
    if not file:
        return jsonify({'error': 'No file provided'}), 400

    file_id = str(uuid.uuid4())
    file_name = file.filename
    file_path = os.path.join('storage', 'temp', file_id)

    # Save file temporarily
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    file.save(file_path)

    # Divide file into chunks
    chunks_info = divide_file_into_chunks(file_path, file_id)

    # Store metadata in SQLite
    db_operations.add_file_record(file_id, file_name, chunks_info)

    # Log chunk replication for debugging
    for chunk in chunks_info:
        print(f"DEBUG: Chunk {chunk['chunk_id']} is replicated across workers: {chunk['worker_ids']}")

    # Send metadata to the Master Node
    try:
        metadata_response = requests.post(
            f"{MASTER_NODE_URL}/metadata",
            json={"file_id": file_id, "file_name": file_name, "chunks": chunks_info}
        )
        if metadata_response.status_code != 200:
            return jsonify({'error': 'Failed to update master node metadata'}), 500
    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Master node communication failed: {str(e)}'}), 500

    # Log the API call
    log_api_call('CREATE', file_id, chunks_info)
    os.remove(file_path)
    return jsonify({'file_id': file_id, 'chunks': chunks_info}), 201

# Read API: Get all files
@app.route('/files', methods=['GET'])
def get_all_files():
    files = db_operations.get_all_files()
    log_api_call('READ', 'all_files', None)
    return jsonify(files), 200

# Read API: Get a file by file ID
@app.route('/files/<file_id>', methods=['GET'])
def get_file(file_id):
    file_info = db_operations.get_file(file_id)
    if not file_info:
        return jsonify({'error': 'File not found'}), 404

    log_api_call('READ', file_id, file_info)
    return jsonify(file_info), 200

# Delete API: Mark file as soft deleted and remove chunks
@app.route('/files/<file_id>', methods=['DELETE'])
def delete_file(file_id):
    file_info = db_operations.get_file(file_id)
    if not file_info:
        return jsonify({'error': 'File not found'}), 404

    # Mark the file as soft-deleted in SQLite
    db_operations.soft_delete_file(file_id)
    

    for chunk in file_info['chunks']:
        chunk_id = chunk['chunk_id']
        for worker_id in chunk['worker_ids']:
            chunk_path = os.path.join(WORKER_STORAGE_PATHS[worker_id], chunk_id)
            try:
                if os.path.exists(chunk_path):
                    os.remove(chunk_path)
                    print(f"DEBUG: Deleted chunk {chunk_id} from {chunk_path}")
                else:
                    print(f"DEBUG: Chunk {chunk_id} not found at {chunk_path}")
            except Exception as e:
                print(f"ERROR: Failed to delete chunk {chunk_id} from {chunk_path}: {e}")

    # Inform the Master Node about the deletion
    try:
        metadata_response = requests.post(
            f"{MASTER_NODE_URL}/metadata",
            json={"file_id": file_id, "file_name": file_info['name'], "chunks": []}
        )
        if metadata_response.status_code != 200:
            return jsonify({'error': 'Failed to update master node on deletion'}), 500
    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Master node communication failed: {str(e)}'}), 500

    # Log the delete action
    log_api_call('DELETE', file_id, file_info)

    return jsonify({'message': f'File {file_id} marked as deleted'}), 200

# Worker Heartbeat API: Relay worker heartbeats to Master Node
@app.route('/heartbeat/<worker_id>', methods=['POST'])
def worker_heartbeat(worker_id):
    try:
        response = requests.post(f"{MASTER_NODE_URL}/heartbeat/{worker_id}")
        return jsonify({'message': f'Heartbeat relayed for worker {worker_id}'}), response.status_code
    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Master node communication failed: {str(e)}'}), 500


@app.route('/')
def index():
    # Get the list of all uploaded files from the database
    files = db_operations.get_all_files()
    return render_template('index.html', files=files)

# Route to handle file upload through the UI
@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files.get('file')
    if not file:
        return redirect(url_for('index'))

    # Save file temporarily to calculate its hash
    temp_path = os.path.join('storage', 'temp', file.filename)
    os.makedirs(os.path.dirname(temp_path), exist_ok=True)

    try:
        file.save(temp_path)
        original_file_hash = calculate_file_hash(temp_path)

        with open(temp_path, 'rb') as temp_file:
            files = {'file': temp_file}
            response = requests.post(f"http://127.0.0.1:5000/files", files=files)

        if response.status_code == 201:
            print(f"DEBUG: File uploaded successfully: {response.json()}")
        else:
            print(f"ERROR: Upload failed: {response.text}")
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

    return redirect(url_for('index'))

@app.route('/files/<file_id>/download', methods=['GET'])
def download_file(file_id):
    """
    Reassembles and downloads the full file by retrieving its chunks from worker nodes.
    """
    file_metadata = db_operations.get_file(file_id)
    if not file_metadata:
        return jsonify({'error': 'File not found'}), 404

    output_dir = os.path.abspath(os.path.join('storage', 'temp'))
    os.makedirs(output_dir, exist_ok=True)
    output_file_path = os.path.join(output_dir, f"{file_id}_reconstructed.txt")

    try:
        with open(output_file_path, 'wb') as output_file:
            for chunk in file_metadata['chunks']:
                chunk_id = chunk['chunk_id']
                chunk_retrieved = False
                for worker_id in chunk['worker_ids']:
                    worker_port = {
                        "worker_1": 5002,
                        "worker_2": 5003,
                        "worker_3": 5004,
                        "worker_4": 5005,
                        "worker_5": 5006
                    }.get(worker_id)

                    chunk_url = f"http://127.0.0.1:{worker_port}/chunks/{chunk_id}"
                    try:
                        chunk_response = requests.get(chunk_url)
                        chunk_response.raise_for_status()
                        output_file.write(chunk_response.content)
                        chunk_retrieved = True
                        print(f"Chunk retrived {chunk_id} from worker {worker_id}")
                        break
                    except requests.exceptions.RequestException as e:
                        print(f"ERROR: Failed to fetch chunk {chunk_id} from worker {worker_id}: {e}")

                if not chunk_retrieved:
                    print(f"ERROR: Could not retrieve chunk {chunk_id} from any worker.")
                    return jsonify({'error': f'Failed to retrieve chunk {chunk_id}'}), 500
    except Exception as e:
        return jsonify({'error': f'Failed to reconstruct file: {e}'}), 500

    return send_file(output_file_path, as_attachment=True, download_name=file_metadata['name'])

if __name__ == '__main__':
    app.run(debug=True, port=5000)
