import paramiko, sys, time
sys.stdout.reconfigure(encoding='utf-8')

HOST = "198.13.184.39"
USER = "root"
PASS = "Alcodome_99"

uploads = [
    ("C:/Project/Hookah/_new_main.py", "/opt/hookah-mixes/main.py"),
    ("C:/Project/Hookah/_new_index.html", "/opt/hookah-mixes/templates/index.html"),
]

print("=== SFTP Upload ===")
transport = paramiko.Transport((HOST, 22))
transport.connect(username=USER, password=PASS)
sftp = paramiko.SFTPClient.from_transport(transport)

for local, remote in uploads:
    print(f"Uploading {local} -> {remote}")
    sftp.put(local, remote)
    print(f"  OK")

sftp.close()
transport.close()
print("Upload complete.\n")

print("=== Restarting server ===")
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, username=USER, password=PASS)

ssh.exec_command("pkill -f 'uvicorn main:app'")
time.sleep(1)
ssh.exec_command("cd /opt/hookah-mixes && nohup uvicorn main:app --host 0.0.0.0 --port 8081 > /tmp/uvicorn.log 2>&1 &")
time.sleep(2)

i, o, e = ssh.exec_command("curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8081/api/mixes")
code = o.read().decode().strip()
print(f"API status: {code}")

if code == '200':
    i, o, e = ssh.exec_command("curl -s http://127.0.0.1:8081/api/mixes | python3 -c \"import json,sys; m=json.load(sys.stdin); [print(x['name'],'→',x.get('pack_method')) for x in m[:5]]\"")
    print(o.read().decode('utf-8', errors='replace').strip())

ssh.close()
print("Done!")
