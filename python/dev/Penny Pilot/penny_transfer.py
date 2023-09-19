import requests
import pandas as pd
from io import StringIO
import os
from datetime import datetime 
import paramiko

# Define the servers
OP_servers = [
    "10.5.1.30",
    "10.5.1.40",
    "10.5.1.50"
#    "10.5.1.61"
]

QA_servers = [
    "10.5.4.64",
    "10.7.8.112",
    "10.7.8.113",
    "10.7.8.114"
]

# Set the username, password and paths for the files
username = "scalp"
password = "Option5Tr@der988"
password2 = "QAtr@de442"
local_file = "/home/elliott/Development/files/penny.cfg"
remote_path = "/home/scalp/cfg/"

# Iterate through the servers and perform the file operations
for server in OP_servers:
    print(f"Connecting to server: {server}")

    # Create an SSH client and connect to the server
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(server, username=username, password=password)

    # Check if the penny.cfg.old file exists and remove it
    stdin, stdout, stderr = ssh_client.exec_command(f"if [ -f {remote_path}pennies.cfg.old ]; then rm {remote_path}pennies.cfg.old; fi")
    stdout.channel.recv_exit_status()

    # Rename the existing penny.cfg file to pennies.cfg.old
    ssh_client.exec_command(f"mv {remote_path}pennies.cfg {remote_path}pennies.cfg.old")

    # Create an SFTP client and upload the penny.cfg file
    sftp_client = ssh_client.open_sftp()
    sftp_client.put(local_file, f"{remote_path}pennies.cfg")

    # Close the SFTP and SSH clients
    sftp_client.close()
    ssh_client.close()

    print(f"File operations completed for server: {server}")

for server in QA_servers:
    print(f"Connecting to server: {server}")

    # Create an SSH client and connect to the server
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(server, username=username, password=password2)

    # Check if the penny.cfg.old file exists and remove it
    stdin, stdout, stderr = ssh_client.exec_command(f"if [ -f {remote_path}pennies.cfg.old ]; then rm {remote_path}pennies.cfg.old; fi")
    stdout.channel.recv_exit_status()

    # Rename the existing penny.cfg file to pennies.cfg.old
    ssh_client.exec_command(f"mv {remote_path}pennies.cfg {remote_path}pennies.cfg.old")

    # Create an SFTP client and upload the penny.cfg file
    sftp_client = ssh_client.open_sftp()
    sftp_client.put(local_file, f"{remote_path}pennies.cfg")

    # Close the SFTP and SSH clients
    sftp_client.close()
    ssh_client.close()

    print(f"File operations completed for server: {server}")
