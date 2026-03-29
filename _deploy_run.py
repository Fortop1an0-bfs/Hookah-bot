import paramiko
import sys

HOST = "198.13.184.39"
PORT = 22
USER = "root"
PASS = "Alcodome_99"

uploads = [
    ("C:/Project/hookah-mixes/main.py", "/opt/hookah-mixes/main.py"),
    ("C:/Project/hookah-mixes/templates/index.html", "/opt/hookah-mixes/templates/index.html"),
]

print("=== SFTP Upload ===")
transport = paramiko.Transport((HOST, PORT))
transport.connect(username=USER, password=PASS)
sftp = paramiko.SFTPClient.from_transport(transport)

for local, remote in uploads:
    print(f"Uploading {local} -> {remote}")
    sftp.put(local, remote)
    print(f"  OK")

sftp.close()
transport.close()
print("Upload complete.\n")

print("=== SSH Commands ===")
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, port=PORT, username=USER, password=PASS)

commands = (
    "systemctl restart hookah-mixes && "
    "sleep 2 && "
    "curl -s http://localhost:8081/api/calendar | head -c 200"
)

stdin, stdout, stderr = ssh.exec_command(commands, timeout=30)
out = stdout.read().decode()
err = stderr.read().decode()

print("STDOUT:")
print(out)
if err:
    print("STDERR:")
    print(err)

ssh.close()
