from flask import Flask, request, jsonify
import os
import uuid
import requests
from shared.utils import log_api_call, divide_file_into_chunks
from database import db_operations

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

if __name__ == '__main__':
    app.run(debug=True, port=5000)
