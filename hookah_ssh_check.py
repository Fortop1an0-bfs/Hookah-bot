import paramiko
import base64
import json
import sys

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('198.13.184.39', username='root', password='Alcodome_99', timeout=15)

def run_b64(cmd):
    stdin, stdout, stderr = client.exec_command(cmd + ' | base64')
    raw = stdout.read()
    err = stderr.read().decode('utf-8', errors='replace')
    decoded = base64.b64decode(raw.strip()).decode('utf-8')
    return decoded, err

sys.stdout.reconfigure(encoding='utf-8')

print("=" * 60)
print("COMMAND 1: web_reviews table")
print("=" * 60)
out1, err1 = run_b64('PGPASSWORD=hookah123 PGCLIENTENCODING=UTF8 psql -U hookah -h 127.0.0.1 -d hookah_db -c "SELECT id, mix_id, bowl_type, rating, comment, smoked_at FROM web_reviews ORDER BY id DESC LIMIT 5;"')
print(out1)
if err1.strip():
    print("STDERR:", err1)

print("=" * 60)
print("COMMAND 2: curl http://localhost:8081/api/calendar")
print("=" * 60)
out2, err2 = run_b64('curl -s http://localhost:8081/api/calendar')
try:
    parsed = json.loads(out2)
    print(json.dumps(parsed, ensure_ascii=False, indent=2))
except Exception:
    print(out2)
if err2.strip():
    print("STDERR:", err2)

print("=" * 60)
print("COMMAND 3: grep calendar in main.py")
print("=" * 60)
out3, err3 = run_b64('grep -n "calendar" /opt/hookah-mixes/main.py -A 20 | head -60')
print(out3)
if err3.strip():
    print("STDERR:", err3)

print("=" * 60)
print("COMMAND 4: calendar/smoked JS in index.html")
print("=" * 60)
out4, err4 = run_b64('grep -n "calendar\\|Calendar\\|smoked" /opt/hookah-mixes/templates/index.html | head -40')
print(out4)
if err4.strip():
    print("STDERR:", err4)

client.close()
print("=" * 60)
print("DONE")
