import paramiko

host = '198.13.184.39'
port = 22
username = 'root'
password = 'Alcodome_99'

local_path = 'C:/Project/Hookah/db/database.py'
remote_path = '/root/hookah-bot/db/database.py'

print('Connecting for SFTP upload...')
transport = paramiko.Transport((host, port))
transport.connect(username=username, password=password)
sftp = paramiko.SFTPClient.from_transport(transport)

# Ensure remote directory exists
remote_dir = '/root/hookah-bot/db'
try:
    sftp.stat(remote_dir)
except FileNotFoundError:
    print(f'Creating remote directory {remote_dir}...')
    sftp.mkdir(remote_dir)

print(f'Uploading {local_path} -> {remote_path}')
sftp.put(local_path, remote_path)
print('Upload complete.')
sftp.close()
transport.close()

print('Connecting via SSH...')
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(host, port=port, username=username, password=password)

stdin, stdout, stderr = client.exec_command(
    'systemctl restart hookah-bot && sleep 3 && systemctl status hookah-bot --no-pager'
)
out = stdout.read().decode()
err = stderr.read().decode()

import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
print('=== STATUS OUTPUT ===')
print(out)
if err:
    print('=== STDERR ===')
    print(err)

client.close()
print('Done.')
