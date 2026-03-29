import paramiko, sys, time
sys.stdout.reconfigure(encoding='utf-8')

c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect('198.13.184.39', username='root', password='Alcodome_99')

# Check if already running
i, o, e = c.exec_command('ps aux | grep uvicorn | grep -v grep')
out = o.read().decode('utf-8', errors='replace').strip()
print("Current state:", out if out else "NOT RUNNING")

if not out:
    start_cmd = 'cd /opt/hookah-mixes && nohup /opt/hookah-mixes/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8081 > /var/log/hookah-mixes.log 2>&1 &'
    i2, o2, e2 = c.exec_command(start_cmd)
    o2.read(); e2.read()
    time.sleep(3)
    i3, o3, e3 = c.exec_command('ps aux | grep uvicorn | grep -v grep')
    print("After start:", o3.read().decode('utf-8', errors='replace').strip())

i4, o4, e4 = c.exec_command('tail -10 /var/log/hookah-mixes.log 2>/dev/null || echo "no log"')
print("Log:", o4.read().decode('utf-8', errors='replace').strip())

c.close()
