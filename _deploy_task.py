import subprocess
import sys

# First install paramiko if needed
subprocess.run([sys.executable, "-m", "pip", "install", "paramiko"], capture_output=True)

import paramiko
import os

HOST = "198.13.184.39"
PORT = 22
USER = "root"
PASSWORD = "Alcodome_99"

UPLOADS = [
    ("C:/Project/hookah-mixes/main.py", "/opt/hookah-mixes/main.py"),
    ("C:/Project/hookah-mixes/templates/index.html", "/opt/hookah-mixes/templates/index.html"),
]

SQL_CMD = """PGPASSWORD=hookah123 psql -U hookah -h 127.0.0.1 -d hookah_db -c "
ALTER TABLE web_mixes ADD COLUMN IF NOT EXISTS strength VARCHAR(20) DEFAULT 'средний';
UPDATE web_mixes SET strength = 'лёгкий' WHERE id IN (3, 8, 12, 14);
UPDATE web_mixes SET strength = 'крепкий' WHERE id IN (5, 7, 9, 16);
UPDATE web_mixes SET strength = 'средний' WHERE id IN (1, 2, 4, 6, 10, 11, 13, 15);
" """

SHELL_CMDS = [
    "systemctl restart hookah-mixes",
    "sleep 2",
    """curl -s http://localhost:8081/api/mixes | python3 -c "import sys,json; d=json.load(sys.stdin); [print(m['name'], '-', m['strength']) for m in d]" """,
]

print("=== Connecting via SSH ===")
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(HOST, port=PORT, username=USER, password=PASSWORD, timeout=30)
print("Connected.")

# --- SFTP Upload ---
print("\n=== SFTP Upload ===")
sftp = client.open_sftp()
for local_path, remote_path in UPLOADS:
    if not os.path.exists(local_path):
        print(f"  [SKIP] Local file not found: {local_path}")
        continue
    # Ensure remote directory exists
    remote_dir = remote_path.rsplit("/", 1)[0]
    try:
        sftp.stat(remote_dir)
    except FileNotFoundError:
        # Create directory recursively via SSH
        stdin, stdout, stderr = client.exec_command(f"mkdir -p {remote_dir}")
        stdout.channel.recv_exit_status()
    sftp.put(local_path, remote_path)
    print(f"  [OK] {local_path} -> {remote_path}")
sftp.close()

# --- SQL Commands ---
print("\n=== Running SQL ===")
stdin, stdout, stderr = client.exec_command(SQL_CMD)
exit_code = stdout.channel.recv_exit_status()
out = stdout.read().decode()
err = stderr.read().decode()
print(f"Exit code: {exit_code}")
if out:
    print("STDOUT:", out)
if err:
    print("STDERR:", err)

# --- Shell Commands ---
print("\n=== Shell Commands ===")
full_cmd = " && ".join(SHELL_CMDS)
stdin, stdout, stderr = client.exec_command(full_cmd)
exit_code = stdout.channel.recv_exit_status()
out = stdout.read().decode()
err = stderr.read().decode()
print(f"Exit code: {exit_code}")
if out:
    print("OUTPUT:\n" + out)
if err:
    print("STDERR:\n" + err)

client.close()
print("\n=== Done ===")
