import paramiko, sys, time
sys.stdout.reconfigure(encoding='utf-8')

c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect('198.13.184.39', username='root', password='Alcodome_99')
sftp = paramiko.SFTPClient.from_transport(c.get_transport())
sftp.put('C:/Project/Hookah/_new_index.html', '/opt/hookah-mixes/templates/index.html')
print("index.html uploaded")
sftp.close()

# Start fresh (don't kill, just start if not running)
i, o, e = c.exec_command('ps aux | grep uvicorn | grep -v grep')
running = o.read().decode().strip()
print("Currently running:", running[:80] if running else "NONE")

if not running:
    start = 'cd /opt/hookah-mixes && nohup /opt/hookah-mixes/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8081 > /var/log/hookah-mixes.log 2>&1 &'
    i2, o2, e2 = c.exec_command(start)
    o2.read(); e2.read()
    time.sleep(4)
    i3, o3, e3 = c.exec_command('ps aux | grep uvicorn | grep -v grep')
    print("After start:", o3.read().decode().strip()[:100])

i4, o4, e4 = c.exec_command('tail -5 /var/log/hookah-mixes.log 2>/dev/null')
print("Log:", o4.read().decode('utf-8', errors='replace').strip())
c.close()
