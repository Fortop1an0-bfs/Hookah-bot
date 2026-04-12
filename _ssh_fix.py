import paramiko, sys, time
sys.stdout.reconfigure(encoding='utf-8')

HOST = '198.13.184.39'
USER = 'root'
PASS = 'Alcodome_99'

c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect(HOST, username=USER, password=PASS)

def run(cmd, desc=''):
    print(f'\n-> {desc or cmd}', flush=True)
    i, o, e = c.exec_command(cmd)
    out = o.read().decode('utf-8', errors='replace').strip()
    err = e.read().decode('utf-8', errors='replace').strip()
    if out: print(out, flush=True)
    if err: print('ERR:', err, flush=True)
    return out

# Check what's on port 8082
run('ss -tlnp | grep 8082 || echo "port 8082 not found"', 'check port 8082')
run('ps aux | grep uvicorn | grep -v grep || echo "no uvicorn"', 'check uvicorn')

# Kill it properly
run('fuser -k 8082/tcp || true', 'fuser kill 8082')
run('pkill -f "uvicorn.*8082" || true', 'pkill uvicorn 8082')
time.sleep(2)
run('ss -tlnp | grep 8082 || echo "port 8082 now free"', 'verify port free')

# Start new
REMOTE_DIR = '/opt/hookah-lab'
VENV_UVICORN = '/opt/hookah-mixes/venv/bin/uvicorn'
run(
    f'cd {REMOTE_DIR} && nohup {VENV_UVICORN} backend:app --host 0.0.0.0 --port 8082 '
    f'> /var/log/hookah-lab.log 2>&1 &',
    'start uvicorn on port 8082'
)
time.sleep(4)
run('ss -tlnp | grep 8082', 'verify port 8082 open')
run('curl -s "http://127.0.0.1:8082/api/tobaccos?q=darkside+supernova" | head -c 500', 'smoke test')

c.close()
print('\n Done!', flush=True)
