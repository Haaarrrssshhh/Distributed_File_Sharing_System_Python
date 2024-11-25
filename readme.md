# Distributed File System (DFS)

A distributed file system (DFS) designed for efficient storage, retrieval, and management of large files across multiple worker nodes, with support for distributed protocols, replication, and fault tolerance.

---

## Features

- **API Gateway**:
  - Exposes REST APIs for file operations: Create, Read, and Delete.
  - Handles file uploads, chunk division, and interaction with worker nodes.

- **Master Node**:
  - Manages metadata for files, chunks, and worker nodes.
  - Handles heartbeat monitoring and tracks worker availability.

- **Worker Nodes**:
  - Stores file chunks and ensures replication across multiple nodes.
  - Sends periodic heartbeats to the Master Node.

- **Distributed Protocols**:
  - Replication of file chunks for fault tolerance.
  - Future support for leader election and consistency models.


### Components

#### API Gateway:
- Entry point for file operations.
- Divides large files into chunks (default size: 128 MB).
- Distributes chunks across worker nodes.

#### Master Node:
- Tracks metadata for files and chunks in SQLite.
- Monitors worker availability using heartbeats.

#### Worker Nodes:
- Stores file chunks in local storage.
- Responds to chunk retrieval and deletion requests.

## Installation

### Prerequisites
- Python 3.8+
- `pip` (Python package manager)
- SQLite (comes pre-installed with Python)

### Setup

1. **Clone the Repository**:
```bash
   git clone https://github.com/Haaarrrssshhh/Distributed_File_System.git
   cd Distributed_File_System
```

2. **Set Up a Virtual Environment**:
```bash
    python3 -m venv dfs_env
    source dfs_env/bin/activate  # On Windows: dfs_env\Scripts\activate
```

3. **Install Dependencies**:
```bash
    pip install -r requirements.txt
```

4. **Initialize the Database**:
```bash
    -Ensure the databases/ folder is created.
    -The system will automatically create dfs_metadata.db and master_metadata.db on startup.
```

5. **Update config.json**:
```bash
    -Make sure the config.json file contains the ports for all master nodes
```


## Usage

### Start the System

1. **Start the Master Node**:
```bash
    python3 distributed_file_system/api_gateway/gateway.py
    python3 distributed_file_system/storage/worker_1/worker1.py
    python3 distributed_file_system/storage/worker_2/worker2.py
    python3 distributed_file_system/storage/worker_3/worker3.py
    python3 distributed_file_system/storage/worker_4/worker4.py
    python3 distributed_file_system/storage/worker_5/worker5.py
```

