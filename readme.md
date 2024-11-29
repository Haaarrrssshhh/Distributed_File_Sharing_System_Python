# Distributed File System (DFS)

A distributed file system (DFS) designed for efficient storage, retrieval, and management of large files across multiple worker nodes, with support for distributed protocols, replication, and fault tolerance.

---


## Demo

Check out our [Demo Video](./videos/demo.mp4) to see the Distributed File System in action!

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

---

## Components

### API Gateway
- **Role**: Acts as the entry point for the UI, handling user requests for uploading and downloading files. It communicates with the master nodes to manage file chunk distribution and retrieval.
- **Functionality**:
  - Divides large files into chunks (default size: 4 MB).
  - Distributes chunks across worker nodes.

### Master Node
- **Role**: Coordinates the overall system operations, manages metadata, and handles the logic for distributing and retrieving file chunks from the worker nodes.
- **Functionality**:
  - Tracks metadata for files and chunks in MongoDB.
  - Monitors worker availability using heartbeats.

### Worker Nodes
- **Role**: Stores and retrieves file chunks as directed by the master servers. They handle the actual data storage and serve chunks upon request.
- **Functionality**:
  - Stores file chunks in local storage.
  - Responds to chunk retrieval and deletion requests.

---

## Installation

### Prerequisites
- **Python**: Version 3.8+
- **`pip`**: Python package manager
- **SQLite**: Comes pre-installed with Python

### Setup

1. **Clone the Repository**:
    ```bash
    git clone https://github.com/YourUsername/Distributed_File_System.git
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
    - Ensure the `databases/` folder is created.

5. **Update `config.json`**:
    - Ensure the `config.json` file contains the ports and IP addresses for all master nodes and worker nodes.

    ```json
    {
        "masters": {
            "master_1": {"ip": "YOUR_EC2_IP_MASTER_1", "port": 5100},
            "master_2": {"ip": "YOUR_EC2_IP_MASTER_2", "port": 5101},
            "master_3": {"ip": "YOUR_EC2_IP_MASTER_3", "port": 5102}
        },
        "workers": {
            "worker_1": {"ip": "YOUR_EC2_IP_WORKER_1", "port": 5001},
            "worker_2": {"ip": "YOUR_EC2_IP_WORKER_2", "port": 5002},
            "worker_3": {"ip": "YOUR_EC2_IP_WORKER_3", "port": 5003},
            "worker_4": {"ip": "YOUR_EC2_IP_WORKER_4", "port": 5004},
            "worker_5": {"ip": "YOUR_EC2_IP_WORKER_5", "port": 5005}
        }
    }
    ```
  6. **Create .env file to store all your variables.**

---

## Usage

### Start the System

1. **Navigate to the Project Directory**:
    ```bash
    cd ~/Distributed_File_System_Python/distributed_file_system
    ```

2. **Activate the Virtual Environment**:
    ```bash
    source dfs_env/bin/activate
    ```

3. **Start the Master Nodes**:
    ```bash
    python3 -m master_node.master master_1
    python3 -m master_node.master master_2
    python3 -m master_node.master master_3
    ```

4. **Start the Worker Nodes**:
    ```bash
    python3 distributed_file_system/storage/worker_1/worker1.py
    python3 distributed_file_system/storage/worker_2/worker2.py
    python3 distributed_file_system/storage/worker_3/worker3.py
    python3 distributed_file_system/storage/worker_4/worker4.py
    python3 distributed_file_system/storage/worker_5/worker5.py
    ```

5. **Start the API Gateway**:
    ```bash
    python3 -m api_gateway.gateway
    ```

### Run All DFS Components Together

#### For Windows:
1. **Navigate to the Project Directory**:
    ```cmd
    cd /d C:\path\to\your\project\Distributed_File_System_Python\distributed_file_system\
    ```

2. **Run the Batch Script**:
    - Open Command Prompt and execute:
      ```cmd
      .\run_all_win.bat
      ```

#### For macOS/Linux:
1. **Navigate to the Project Directory**:
    ```bash
    cd "/path/to/your/project/Distributed_File_System_Python/distributed_file_system"
    ```

2. **Make the Shell Script Executable**:
    ```bash
    chmod +x run_all_mac_or_linux.sh
    ```

3. **Run the Shell Script**:
    ```bash
    ./run_all_mac_or_linux.sh
    ```

---

## Deployment

Deploying the Distributed File System (DFS) on AWS EC2 involves setting up the necessary infrastructure, configuring security groups, and launching the required instances. Below are the detailed steps to deploy DFS using AWS.

### 1. AWS Infrastructure Setup

#### a. Create a Virtual Private Cloud (VPC)

1. **Navigate to the VPC Dashboard** in the AWS Management Console.
2. **Create a New VPC**:
   - **Name:** `DFS_VPC`
   - **IPv4 CIDR Block:** `10.0.0.0/16`
   - **No IPv6 CIDR Block**
   - **Tenancy:** Default
3. **Create the VPC**.

#### b. Create Subnets

1. **Public Subnet**:
   - **Name:** `DFS_Public_Subnet`
   - **Availability Zone:** Choose one (e.g., `us-east-1a`)
   - **IPv4 CIDR Block:** `10.0.1.0/24`
   - **Auto-assign Public IPv4 Address:** Enable

2. **Private Subnet**:
   - **Name:** `DFS_Private_Subnet`
   - **Availability Zone:** Same as Public Subnet for simplicity
   - **IPv4 CIDR Block:** `10.0.2.0/24`
   - **Auto-assign Public IPv4 Address:** Disable

#### c. Create an Internet Gateway (IGW)

1. **Navigate to Internet Gateways** in the VPC Dashboard.
2. **Create Internet Gateway**:
   - **Name:** `DFS_IGW`
3. **Attach IGW to VPC**:
   - Select `DFS_IGW` and attach it to `DFS_VPC`.

#### d. Create Route Tables

1. **Public Route Table**:
   - **Name:** `DFS_Public_Route_Table`
   - **Associated VPC:** `DFS_VPC`
   - **Routes**:
     - **Destination:** `0.0.0.0/0`
     - **Target:** `DFS_IGW`
   - **Associate with Public Subnet**: `DFS_Public_Subnet`

2. **Private Route Table**:
   - **Name:** `DFS_Private_Route_Table`
   - **Associated VPC:** `DFS_VPC`
   - **Routes**:
     - Initially, no routes other than local.
   - **Associate with Private Subnet**: `DFS_Private_Subnet`

#### e. Create a NAT Gateway (For Private Subnet Internet Access)

1. **Allocate an Elastic IP**:
   - Navigate to **Elastic IPs** in the EC2 Dashboard.
   - **Allocate a new Elastic IP** and note its Allocation ID.

2. **Create NAT Gateway**:
   - Navigate to **NAT Gateways** in the VPC Dashboard.
   - **Create NAT Gateway**:
     - **Name:** `DFS_NAT_Gateway`
     - **Subnet:** `DFS_Public_Subnet`
     - **Elastic IP Allocation ID:** Select the allocated Elastic IP.

3. **Update Private Route Table**:
   - **Add Route**:
     - **Destination:** `0.0.0.0/0`
     - **Target:** `DFS_NAT_Gateway`

### 2. Security Groups Configuration

#### a. Gateway Security Group

1. **Navigate to Security Groups** in the EC2 Dashboard.
2. **Create Security Group**:
   - **Name:** `SG_Gateway`
   - **VPC:** `DFS_VPC`

3. **Inbound Rules**:
   - **HTTP (Port 5000)**:
     - **Type:** Custom TCP
     - **Protocol:** TCP
     - **Port Range:** 5000
     - **Source:** `0.0.0.0/0`
     - **Description:** Allow inbound HTTP traffic for UI access.
   - **ICMP (Ping)**:
     - **Type:** All ICMP - IPv4
     - **Protocol:** ICMP
     - **Port Range:** N/A
     - **Source:** `0.0.0.0/0`
     - **Description:** Allow ping from any source.
   - **SSH (Port 22)**:
     - **Type:** SSH
     - **Protocol:** TCP
     - **Port Range:** 22
     - **Source:** `<YOUR_IP_ADDRESS>/32`  <!-- Replace with your IP when deploying -->
     - **Description:** Allow SSH access from your IP.

4. **Outbound Rules**:
   - **All Traffic Allowed** (`0.0.0.0/0`)

#### b. Master and Worker Security Group

1. **Create Security Group**:
   - **Name:** `SG_Masters_Workers`
   - **VPC:** `DFS_VPC`

2. **Inbound Rules**:
   - **Custom TCP (Ports 5000-5400)**:
     - **Type:** Custom TCP
     - **Protocol:** TCP
     - **Port Range:** 5000-5400
     - **Source:** `10.0.0.0/16` (VPC CIDR)
     - **Description:** Allow internal communication among masters, workers, and gateway.
   - **ICMP (Ping)**:
     - **Type:** All ICMP - IPv4
     - **Protocol:** ICMP
     - **Port Range:** N/A
     - **Source:** `10.0.0.0/16`
     - **Description:** Allow ping within the VPC.
   - **SSH (Port 22)**:
     - **Type:** SSH
     - **Protocol:** TCP
     - **Port Range:** 22
     - **Source:** `<BASTION_SG_OR_YOUR_IP>/32`  <!-- Replace with Bastion SG or your IP -->
     - **Description:** Allow SSH access from Bastion Host or your IP.

3. **Outbound Rules**:
   - **All Traffic Allowed** (`0.0.0.0/0`)

#### c. Bastion Host Security Group (Optional but Recommended)

1. **Create Security Group**:
   - **Name:** `SG_Bastion`
   - **VPC:** `DFS_VPC`

2. **Inbound Rules**:
   - **SSH (Port 22)**:
     - **Type:** SSH
     - **Protocol:** TCP
     - **Port Range:** 22
     - **Source:** `<YOUR_IP_ADDRESS>/32`  <!-- Replace with your IP when deploying -->
     - **Description:** Allow SSH access from your IP.

3. **Outbound Rules**:
   - **All Traffic Allowed** (`0.0.0.0/0`)

### 3. Launch EC2 Instances

#### a. Launch the API Gateway Instance

1. **Navigate to EC2 Dashboard** and click **Launch Instance**.
2. **Configure Instance**:
   - **Name:** `DFS_Gateway`
   - **AMI:** Ubuntu Server 20.04 LTS (or your preferred Linux distribution)
   - **Instance Type:** `t2.micro` (suitable for testing; scale as needed)
   - **Network:** `DFS_VPC`
   - **Subnet:** `DFS_Public_Subnet`
   - **Auto-assign Public IP:** Enabled
   - **Security Group:** `SG_Gateway`
3. **Add Storage**: Default is sufficient for testing.
4. **Add Tags** (optional):
   - **Key:** `Role`
   - **Value:** `Gateway`
5. **Review and Launch**.
6. **Download Key Pair** if not already available.

#### b. Launch Master Instances

1. **Repeat the above steps for each Master Node**:
   - **Name:** `DFS_Master_1`, `DFS_Master_2`, `DFS_Master_3`
   - **AMI:** Same as Gateway
   - **Instance Type:** As required
   - **Network:** `DFS_VPC`
   - **Subnet:** `DFS_Private_Subnet`
   - **Auto-assign Public IP:** Disabled
   - **Security Group:** `SG_Masters_Workers`
2. **Add Tags**:
   - **Key:** `Role`
   - **Value:** `Master`

#### c. Launch Worker Instances

1. **Repeat the above steps for each Worker Node**:
   - **Name:** `DFS_Worker_1`, `DFS_Worker_2`, ..., `DFS_Worker_5`
   - **AMI:** Same as Gateway
   - **Instance Type:** As required
   - **Network:** `DFS_VPC`
   - **Subnet:** `DFS_Private_Subnet`
   - **Auto-assign Public IP:** Disabled
   - **Security Group:** `SG_Masters_Workers`
2. **Add Tags**:
   - **Key:** `Role`
   - **Value:** `Worker`

### 4. (Optional) Set Up a Bastion Host for SSH Access

1. **Launch a Bastion Host Instance** in the `DFS_Public_Subnet`:
   - **Name:** `DFS_Bastion`
   - **AMI:** Ubuntu Server 22.04 LTS
   - **Instance Type:** `t2.micro`
   - **Network:** `DFS_VPC`
   - **Subnet:** `DFS_Public_Subnet`
   - **Auto-assign Public IP:** Enabled
   - **Security Group:** `SG_Bastion`

2. **Configure SSH Access**:
   - Use SSH keys to securely access the Bastion Host.
   - From the Bastion Host, SSH into Master and Worker instances using their private IPs.

---

## Testing and Verification

### 1. Verify Service Status on Instances

#### a. Check if Flask Applications are Running

- **On Each Instance (Master and Worker)**:
    ```bash
    ps aux | grep <service_name>.py
    ```
    - Replace `<service_name>` with `master` or `worker` as applicable.
    - **Expected Output**: A line indicating that the Python script is running.

#### b. Verify Listening Ports

- **On Each Instance**:
    ```bash
    sudo netstat -tulpn | grep :<PORT>
    ```
    - Replace `<PORT>` with the respective port number (e.g., `5003` for `worker_3`).
    - **Expected Output**: A line showing the service listening on `0.0.0.0:<PORT>`.

### 2. Test `/health` Endpoint

- **From the API Gateway Server or Another Instance within the VPC**:
    ```bash
    curl http://<INSTANCE_PRIVATE_IP>:<PORT>/health
    ```
    - **Example**:
        ```bash
        curl http://10.0.1.26:5003/health
        ```
    - **Expected Response**:
        ```json
        {"status": "ok"}
        ```

- **Using Telnet**:
    ```bash
    telnet <INSTANCE_PRIVATE_IP> <PORT>
    ```
    - **Example**:
        ```bash
        telnet 10.0.1.26 5003
        ```
    - **Expected Outcome**: Successful connection if the service is running and accessible.

### 3. Troubleshooting Steps if Tests Fail

- **Ensure Services are Running**: Start or restart Flask applications as needed.
- **Verify Flask Configuration**: Confirm that `host='0.0.0.0'` and the correct port are set.
- **Check Security Groups**: Ensure inbound rules allow necessary traffic.
- **Inspect OS-Level Firewalls**: Adjust or disable firewalls temporarily for testing.
- **Review Application Logs**: Look for errors or exceptions in `worker.log`, `master.log`, or `gateway.log`.


---


