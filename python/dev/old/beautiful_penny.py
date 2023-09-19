import requests
import os
import pandas as pd
import paramiko

def download_file(url, file_path):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36'
    }
    response = requests.get(url, headers=headers, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024
    progress = 0

    with open(file_path, "wb") as f:
        for data in response.iter_content(block_size):
            progress += len(data)
            f.write(data)
            completed = int(50 * progress / total_size)
            print(f"\rDownloading: [{'#' * completed}{'.' * (50 - completed)}] {completed * 2}%", end='')
    print()

def read_excel_to_dataframe(file_path):
    df = pd.read_excel(file_path, engine='openpyxl', header=None)
    df = df.iloc[9:, 0:1]  # Keep rows from index 6 to the end and only the first column
    df.reset_index(drop=True, inplace=True)  # Reset the index
    df = df.dropna()
    return df

def send_files(df, output_file='/home/elliott/Development/files/penny.cfg'):
    df.to_csv(output_file, index=False, header=False, line_terminator='\n', mode='w')

    # Define the servers
    servers = [
        "10.5.1.28"
        # "10.5.1.30",
        # "10.5.1.40",
        # "10.5.1.50",
        # "10.5.1.61",
        # "10.5.4.64"
    ]

    # Set the username, password and paths for the files
    username = "scalp"
    password = "QAtr@de442"
    local_file = "/home/elliott/Development/files/penny.cfg"
    remote_path = "/home/scalp/cfg/"

    # Iterate through the servers and perform the file operations
    for server in servers:
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

def main():
    file_url = 'https://www.nasdaq.com/pennyprogram'
    file_name = "pennyprogram.xlsx"

    # You can change the directory to your desired location
    save_directory = "/home/elliott/Development/files/"
    os.makedirs(save_directory, exist_ok=True)
    file_path = os.path.join(save_directory, file_name)

    file_url = 'https://www.nasdaq.com/pennyprogram'
    file_name = "pennyprogram.xlsx"

    save_directory = "/home/elliott/Development/files/"
    os.makedirs(save_directory, exist_ok=True)
    file_path = os.path.join(save_directory, file_name)

    download_file(file_url, file_path)

    df = read_excel_to_dataframe(file_path)
    print(df)

    send_files(df)

if __name__ == '__main__':
    main()
    
