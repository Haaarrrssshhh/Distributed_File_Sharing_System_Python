@echo off

:: Navigate to the project root (Update the path below to where you cloned the project)
cd /d C:\Users\achal\DS-project\Distributed_File_System_Python\distributed_file_system\

:: Open each terminal with the correct working directory
start cmd /k "python -m api_gateway.gateway"
start cmd /k "python -m master_node.master master_1"
start cmd /k "python -m master_node.master master_2"
start cmd /k "python -m master_node.master master_3"
start cmd /k "python storage/worker_1/worker1.py"
start cmd /k "python storage/worker_2/worker2.py"
start cmd /k "python storage/worker_3/worker3.py"
start cmd /k "python storage/worker_4/worker4.py"
start cmd /k "python storage/worker_5/worker5.py"
