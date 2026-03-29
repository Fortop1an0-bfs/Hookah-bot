import paramiko
import sys

host = "198.13.184.39"
user = "root"
password = "Alcodome_99"

files = [
    (r"C:/Project/Hookah/bot/agents/internet_searcher.py", "/opt/hookah/bot/agents/internet_searcher.py"),
    (r"C:/Project/Hookah/bot/agents/research_agent.py",    "/opt/hookah/bot/agents/research_agent.py"),
    (r"C:/Project/Hookah/bot/handlers/research_handler.py", "/opt/hookah/bot/handlers/research_handler.py"),
]

print(f"Connecting to {host}...")
transport = paramiko.Transport((host, 22))
transport.connect(username=user, password=password)
sftp = paramiko.SFTPClient.from_transport(transport)
print("Connected.")

def ensure_remote_dir(sftp, remote_dir):
    parts = remote_dir.strip("/").split("/")
    path = ""
    for p in parts:
        path += "/" + p
        try:
            sftp.stat(path)
        except FileNotFoundError:
            sftp.mkdir(path)

upload_ok = True
for local, remote in files:
    try:
        remote_dir = remote.rsplit("/", 1)[0]
        ensure_remote_dir(sftp, remote_dir)
        sftp.put(local, remote)
        print(f"OK   {local} -> {remote}")
    except Exception as e:
        print(f"ERR  {local}: {e}")
        upload_ok = False

sftp.close()
transport.close()

if not upload_ok:
    print("UPLOAD: some files failed")
    sys.exit(1)
else:
    print("UPLOAD: all files transferred successfully")

# SSH to run post-upload commands
print("\nRunning post-upload commands via SSH...")
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(host, username=user, password=password)

commands = "systemctl restart hookah-bot && sleep 2 && systemctl status hookah-bot --no-pager"
stdin, stdout, stderr = ssh.exec_command(commands)
out = stdout.read().decode()
err = stderr.read().decode()
exit_code = stdout.channel.recv_exit_status()

print(f"Exit code: {exit_code}")
if out:
    print("STDOUT:\n" + out)
if err:
    print("STDERR:\n" + err)

ssh.close()
print("Done.")
