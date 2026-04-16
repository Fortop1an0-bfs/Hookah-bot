import paramiko, sys, time
sys.stdout.reconfigure(encoding='utf-8')

HOST, USER, PASS = '198.13.184.39', 'root', 'Alcodome_99'
REMOTE = '/opt/scraper'
VENV_PY = '/opt/hookah-mixes/venv/bin/python'

c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect(HOST, username=USER, password=PASS)

# Upload scraper
sftp = c.open_sftp()
try: sftp.mkdir(REMOTE)
except: pass
sftp.put('fast_scraper.py', f'{REMOTE}/fast_scraper.py')
sftp.close()
print('✓ uploaded fast_scraper.py')

# Write a start script on server
start_sh = f"""#!/bin/bash
pkill -f fast_scraper 2>/dev/null
pkill -f run_priority 2>/dev/null
sleep 1
cd {REMOTE}
setsid {VENV_PY} fast_scraper.py >> /var/log/htr_fast.log 2>&1 &
echo $! > /var/run/htr_scraper.pid
echo "started PID=$(cat /var/run/htr_scraper.pid)"
"""
sftp2 = c.open_sftp()
with sftp2.open('/opt/scraper/start.sh', 'w') as f:
    f.write(start_sh)
sftp2.close()

# Make executable and run it, reading output with short timeout
i, o, e = c.exec_command('chmod +x /opt/scraper/start.sh && /opt/scraper/start.sh', timeout=8)
try:
    result = o.read().decode('utf-8', errors='replace').strip()
    print(f'✓ {result}')
except Exception as ex:
    print(f'start sent (timeout): {ex}')

time.sleep(6)

# Quick status check
i2, o2, e2 = c.exec_command(
    'pgrep -fa fast_scraper 2>/dev/null | grep -v grep; '
    'echo "---"; tail -8 /var/log/htr_fast.log 2>/dev/null; '
    'echo "---"; PGPASSWORD=hookah123 psql -h localhost -U hookah -d hookah_db -t -c '
    '"SELECT COUNT(*) FROM scraper.htr_tobaccos;" 2>/dev/null',
    timeout=10
)
try:
    print(o2.read().decode('utf-8', errors='replace'))
except: pass

c.close()
