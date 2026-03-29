import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import paramiko

HOST = "198.13.184.39"
PORT = 22
USER = "root"
PASSWORD = "Alcodome_99"

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(HOST, port=PORT, username=USER, password=PASSWORD, timeout=30)

cmd = 'curl -s http://localhost:8081/api/mixes | python3 -c "import sys,json; d=json.load(sys.stdin); [print(m[\'name\'], \'-\', m[\'strength\']) for m in d]"'
stdin, stdout, stderr = client.exec_command(cmd)
exit_code = stdout.channel.recv_exit_status()
out = stdout.read().decode('utf-8')
err = stderr.read().decode('utf-8')
print("=== curl output ===")
print(out)
if err:
    print("STDERR:", err)
client.close()
