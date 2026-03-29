import paramiko
import sys

HOST = "198.13.184.39"
PORT = 22
USER = "root"
PASS = "Alcodome_99"

LOCAL_MAIN = "C:/Project/hookah-mixes/main.py"
LOCAL_SCHEMA = "C:/Project/hookah-mixes/schema.sql"
REMOTE_MAIN = "/opt/hookah-mixes/main.py"
REMOTE_SCHEMA = "/opt/hookah-mixes/schema.sql"

print("=== Connecting via SSH ===")
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(HOST, port=PORT, username=USER, password=PASS, timeout=30)
print("Connected.")

# SFTP Upload
print("\n=== SFTP Upload ===")
sftp = client.open_sftp()

# Ensure remote directory exists
try:
    sftp.stat("/opt/hookah-mixes")
except FileNotFoundError:
    print("Creating /opt/hookah-mixes/")
    stdin, stdout, stderr = client.exec_command("mkdir -p /opt/hookah-mixes")
    stdout.channel.recv_exit_status()

print(f"Uploading {LOCAL_MAIN} -> {REMOTE_MAIN}")
sftp.put(LOCAL_MAIN, REMOTE_MAIN)
print("Done: main.py")

print(f"Uploading {LOCAL_SCHEMA} -> {REMOTE_SCHEMA}")
sftp.put(LOCAL_SCHEMA, REMOTE_SCHEMA)
print("Done: schema.sql")

sftp.close()
print("SFTP uploads complete.")

# Run commands
commands = [
    ("psql -U hookah -d hookah_db -f /opt/hookah-mixes/schema.sql", 30),
    ("systemctl restart hookah-mixes", 15),
    ("sleep 2 && curl -s http://localhost:8081/api/mixes | head -c 500", 20),
]

for cmd, timeout in commands:
    print(f"\n=== Running: {cmd} ===")
    stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    exit_status = stdout.channel.recv_exit_status()
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    print(f"Exit code: {exit_status}")
    if out:
        print("STDOUT:")
        print(out)
    if err:
        print("STDERR:")
        print(err)

client.close()
print("\n=== Done ===")
