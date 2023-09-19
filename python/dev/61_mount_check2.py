import os

# Set the path to the remote drive and the local mount point
remote_path = "/opt/storage/bookreaderdata"
local_path = "/home/scalp/bookReader"
remote_path2 = "/volume1/Storage"
local_path2 = "/mnt/nystorage"

# Check if the first remote drive is already mounted
if os.path.ismount(f"{local_path}/bulkinput") and os.path.ismount(f"{local_path}/bulkoutput"):
    print("First remote drive already mounted")
else:
    print("First remote drive not mounted")
    print("Mounting...")
    # Mount the first remote drive using SSHFS
    sshfs_command = f"sudo sshfs scalp@10.5.1.28:{remote_path}/input {local_path}/bulkinput && sshfs scalp@10.5.1.28:{remote_path}/output {local_path}/bulkoutput"
    os.system(sshfs_command)
    print("First remote drive mounted successfully")        

# Check if the second remote drive is already mounted
if os.path.ismount(f"{local_path2}"):
    print("Second remote drive already mounted")
else:
    print("Second remote drive not mounted")
    print("Mounting...")
    # Mount the second remote drive using SSHFS
    sshfs_command2 = f"sudo sshfs scalp@10.7.0.20:{remote_path2}/ {local_path2}/"
    os.system(sshfs_command2)
    print("Second remote drive mounted successfully")
