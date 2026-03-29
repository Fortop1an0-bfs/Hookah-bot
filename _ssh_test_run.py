import paramiko
import sys

sys.stdout.reconfigure(encoding='utf-8')

c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect('198.13.184.39', username='root', password='Alcodome_99')

# Fix: patch main.py on the server — convert smoked_at string to datetime.date
fix_cmd = r"""
cd /opt/hookah-mixes && python3 - << 'PYEOF'
import re

with open('main.py', 'r') as f:
    content = f.read()

# Add datetime import if not present
if 'import datetime' not in content and 'from datetime' not in content:
    content = content.replace('import os\n', 'import os\nimport datetime\n', 1)

# Fix smoked_at: convert string to date object before passing to asyncpg
old = "    smoked_at = data.get(\"smoked_at\") or None"
new = (
    "    smoked_at_raw = data.get(\"smoked_at\") or None\n"
    "    if smoked_at_raw:\n"
    "        try:\n"
    "            smoked_at = datetime.date.fromisoformat(smoked_at_raw)\n"
    "        except (ValueError, TypeError):\n"
    "            smoked_at = None\n"
    "    else:\n"
    "        smoked_at = None"
)

if old in content:
    content = content.replace(old, new)
    with open('main.py', 'w') as f:
        f.write(content)
    print("PATCHED OK")
else:
    print("PATTERN NOT FOUND — current smoked_at line:")
    for i, line in enumerate(content.splitlines(), 1):
        if 'smoked_at' in line:
            print(f"  {i}: {line}")
PYEOF
"""

commands = [
    ("=== Apply fix to main.py ===", fix_cmd),
    ("=== Restart hookah-mixes service ===",
     "systemctl restart hookah-mixes && sleep 2 && systemctl is-active hookah-mixes"),
    ("=== 1. POST review with date ===",
     'curl -s -X POST http://localhost:8081/api/reviews '
     '-H "Content-Type: application/json" '
     '-d \'{"mix_id":1,"bowl_type":"убивашка","rating":5,"comment":"Тест проверки","smoked_at":"2026-03-29"}\''),
    ("=== 2. Check DB ===",
     'PGPASSWORD=hookah123 psql -U hookah -h 127.0.0.1 -d hookah_db '
     '-c "SELECT id, mix_id, bowl_type, rating, comment, smoked_at, created_at FROM web_reviews ORDER BY id DESC LIMIT 3;"'),
    ("=== 3. Calendar API ===",
     'curl -s http://localhost:8081/api/calendar'),
    ("=== 4. Reviews API for mix 1 ===",
     'curl -s http://localhost:8081/api/reviews/1'),
]

for label, cmd in commands:
    print(label)
    stdin, stdout, stderr = c.exec_command(cmd)
    out = stdout.read().decode('utf-8')
    err = stderr.read().decode('utf-8')
    print(out)
    if err:
        print("STDERR:", err)
    print()

c.close()
