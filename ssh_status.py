import paramiko
import sys

host = "198.13.184.39"
user = "root"
password = "Alcodome_99"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(host, username=user, password=password)

commands = "systemctl restart hookah-bot && sleep 2 && systemctl status hookah-bot --no-pager"
stdin, stdout, stderr = ssh.exec_command(commands)
out = stdout.read().decode("utf-8", errors="replace")
err = stderr.read().decode("utf-8", errors="replace")
exit_code = stdout.channel.recv_exit_status()

sys.stdout.buffer.write(f"Exit code: {exit_code}\n".encode("utf-8"))
if out:
    sys.stdout.buffer.write(b"STDOUT:\n" + out.encode("utf-8") + b"\n")
if err:
    sys.stdout.buffer.write(b"STDERR:\n" + err.encode("utf-8") + b"\n")

ssh.close()
