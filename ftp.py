import paramiko
import os
from pathlib import Path,PureWindowsPath

# create ssh client 
ssh_client = paramiko.SSHClient()

# remote server credentials
host = '10.11.164.62'
username = 'achatt0'
password = 'November@123456'
port = '22'

ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_client.connect(hostname=host,port=port,username=username,password=password)

data_folder = PureWindowsPath("P:\\KIS\\Continuous Improvement Reporting Team\\LocalFalconFiles\\")
remote_folder = "/data/opt/WinshareMSO/Local Falcon Files/"
ftp = ssh_client.open_sftp()

for f in ftp.listdir(remote_folder):    
	remote_path = os.path.join(remote_folder,f)
	local_path = os.path.join(data_folder,f)
	
	try:		
		ftp.get(remote_path,local_path)
		print(f"Downloaded: {remote_path} to {local_path}")		
	except FileNotFoundError:
		print(f"File not found on the remote server: {remote_path}")
	except Exception as e:
		print(f"Error downloading {remote_path}: {e}")
# close the connection
ftp.close()
ssh_client.close()