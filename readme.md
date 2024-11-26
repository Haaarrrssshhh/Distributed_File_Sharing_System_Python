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
    python3 -m api_gateway.gateway
    python3 -m master_node.master master_1
    python3 -m master_node.master master_2
    python3 -m master_node.master master_3
    python3 distributed_file_system/storage/worker_1/worker1.py
    python3 distributed_file_system/storage/worker_2/worker2.py
    python3 distributed_file_system/storage/worker_3/worker3.py
    python3 distributed_file_system/storage/worker_4/worker4.py
    python3 distributed_file_system/storage/worker_5/worker5.py
```

### Run all dfs components together

```bash
  1. Navigate to Project Path:  /Distributed_File_System_Python/distributed_file_system
  2. Update the Project Path in the Scripts: The location where you cloned or stored the project

    Windows: Open the "run_all_win.bat" file and update the project path:
      cd /d C:\path\to\your\project\Distributed_File_System_Python\distributed_file_system\

    macOS/Linux: Open the "run_all_mac_or_linux.sh" file and update the project path:
      cd "/path/to/your/project/Distributed_File_System_Python/distributed_file_system" || exit


  3. Execution Instructions
    Windows: 
      Navigate to the directory containing run_all_win.bat.
      Run in Command Prompt: ".\run_all_win.bat" or "run_all_win.bat"

    For macOS/Linux:
      Navigate to the directory containing "run_all_mac_or_linux.sh".
      Make the run_all.sh executable: " chmod +x run_all_mac_or_linux.sh"
      Run the script using the  command: "./run_all_mac_or_linux.sh "
```
