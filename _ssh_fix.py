import paramiko
import sys

# Force UTF-8 output
sys.stdout.reconfigure(encoding='utf-8')

host = "198.13.184.39"
user = "root"
password = "Alcodome_99"

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(host, username=user, password=password, timeout=30)

commands = [
    # Fix duplicate: remove the extra 'await loadCalendar();' line that was inserted twice
    r"""python3 -c "
import re
with open('/opt/hookah-mixes/templates/index.html', 'r') as f:
    content = f.read()
# Replace double occurrence of await loadCalendar(); with single
fixed = content.replace('    await loadCalendar();\n    await loadCalendar();', '    await loadCalendar();')
with open('/opt/hookah-mixes/templates/index.html', 'w') as f:
    f.write(fixed)
print('done')
" """,
    r"""grep -n "openModal\|loadCalendar\|renderCalendar\|showPage" /opt/hookah-mixes/templates/index.html | tail -20""",
    r"""systemctl restart hookah-mixes && sleep 2 && systemctl status hookah-mixes --no-pager | head -5""",
]

for cmd in commands:
    print(f"\n=== CMD: {cmd[:80]} ===")
    stdin, stdout, stderr = client.exec_command(cmd, timeout=30)
    out = stdout.read().decode('utf-8', errors='replace')
    err = stderr.read().decode('utf-8', errors='replace')
    if out:
        print(out)
    if err:
        print("STDERR:", err)

client.close()
print("Done.")
