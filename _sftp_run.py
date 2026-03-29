import paramiko
import sys

HOST = "198.13.184.39"
PORT = 22
USER = "root"
PASS = "Alcodome_99"

uploads = [
    ("C:/Project/hookah-mixes/main.py",              "/opt/hookah-mixes/main.py"),
    ("C:/Project/hookah-mixes/templates/index.html", "/opt/hookah-mixes/templates/index.html"),
    ("C:/Project/hookah-mixes/schema.sql",           "/opt/hookah-mixes/schema.sql"),
]

commands = [
    'PGPASSWORD=hookah123 psql -U hookah -h 127.0.0.1 -d hookah_db -c "ALTER TABLE web_reviews ADD COLUMN IF NOT EXISTS smoked_at DATE;"',
    "systemctl restart hookah-mixes",
    "sleep 2",
    "systemctl status hookah-mixes --no-pager | head -5",
]

print("=== Connecting ===")
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(HOST, port=PORT, username=USER, password=PASS, timeout=30)
print("Connected.")

# SFTP upload
print("\n=== SFTP Upload ===")
sftp = client.open_sftp()
for local, remote in uploads:
    remote_dir = remote.rsplit("/", 1)[0]
    try:
        sftp.stat(remote_dir)
    except FileNotFoundError:
        stdin, stdout, stderr = client.exec_command(f"mkdir -p {remote_dir}")
        stdout.channel.recv_exit_status()
    sftp.put(local, remote)
    print(f"  Uploaded: {local} -> {remote}")
sftp.close()
print("SFTP uploads complete.")

# SSH commands
print("\n=== SSH Commands ===")
for cmd in commands:
    print(f"\n$ {cmd}")
    stdin, stdout, stderr = client.exec_command(cmd, timeout=30)
    exit_status = stdout.channel.recv_exit_status()
    out = stdout.read().decode()
    err = stderr.read().decode()
    if out:
        print(out, end="")
    if err:
        print("[stderr]:", err, end="")
    print(f"[exit: {exit_status}]")

client.close()
print("\n=== Done ===")
