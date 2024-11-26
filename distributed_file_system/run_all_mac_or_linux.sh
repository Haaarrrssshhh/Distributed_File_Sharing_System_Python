#!/bin/bash

# Navigate to the project root (Update the path below to where you cloned the project)
cd "/Users/achal/DS-project/Distributed_File_System_Python/distributed_file_system" || exit

# Open each command in a new terminal window
gnome-terminal -- bash -c "python3 -m api_gateway.gateway; exec bash"
gnome-terminal -- bash -c "python3 -m master_node.master master_1; exec bash"
gnome-terminal -- bash -c "python3 -m master_node.master master_2; exec bash"
gnome-terminal -- bash -c "python3 -m master_node.master master_3; exec bash"
gnome-terminal -- bash -c "python3 storage/worker_1/worker1.py; exec bash"
gnome-terminal -- bash -c "python3 storage/worker_2/worker2.py; exec bash"
gnome-terminal -- bash -c "python3 storage/worker_3/worker3.py; exec bash"
gnome-terminal -- bash -c "python3 storage/worker_4/worker4.py; exec bash"
gnome-terminal -- bash -c "python3 storage/worker_5/worker5.py; exec bash"