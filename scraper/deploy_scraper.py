"""Deploy and run htreviews scraper on server"""
import paramiko
import sys

HOST = "198.13.184.39"
PORT = 22
USER = "root"
PASS = "Alcodome_99"

REMOTE_DIR = "/opt/scraper"

uploads = [
    ("C:/Project/Hookah/scraper/htreviews_scraper.py", f"{REMOTE_DIR}/htreviews_scraper.py"),
    ("C:/Project/Hookah/scraper/requirements.txt", f"{REMOTE_DIR}/requirements.txt"),
]

print("=== SFTP Upload ===")
transport = paramiko.Transport((HOST, PORT))
transport.connect(username=USER, password=PASS)
sftp = paramiko.SFTPClient.from_transport(transport)

# Create remote dir if not exists
try:
    sftp.mkdir(REMOTE_DIR)
    print(f"Created {REMOTE_DIR}")
except OSError:
    pass  # already exists

for local, remote in uploads:
    print(f"Uploading {local} -> {remote}")
    sftp.put(local, remote)
    print("  OK")

sftp.close()
transport.close()
print("Upload complete.\n")

print("=== SSH: Install deps & run scraper ===")
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, port=PORT, username=USER, password=PASS)

commands = [
    f"pip3 install -r {REMOTE_DIR}/requirements.txt -q",
    f"cd {REMOTE_DIR} && nohup python3 htreviews_scraper.py > scraper.log 2>&1 &",
    "echo 'Scraper started. PID:' $!",
]

for cmd in commands:
    print(f"$ {cmd}")
    stdin, stdout, stderr = ssh.exec_command(cmd)
    out = stdout.read().decode('utf-8', errors='replace').strip()
    err = stderr.read().decode('utf-8', errors='replace').strip()
    if out:
        print(f"  OUT: {out}")
    if err:
        print(f"  ERR: {err}")

ssh.close()
print("\nDone. Check logs: ssh root@198.13.184.39 'tail -f /opt/scraper/scraper.log'")
