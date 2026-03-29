import paramiko
import sys

sys.stdout.reconfigure(encoding='utf-8')

c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect('198.13.184.39', username='root', password='Alcodome_99')

cmds = [
    "sed -i \"s/page==='calendar' ? '' : 'none'/page==='calendar' ? 'block' : 'none'/\" /opt/hookah-mixes/templates/index.html",
    "sed -i \"s/page==='mixes' ? '' : 'none'/page==='mixes' ? 'block' : 'none'/\" /opt/hookah-mixes/templates/index.html",
    r"grep -n 'calPage\|mixesPage' /opt/hookah-mixes/templates/index.html | grep 'style\|display'",
]

for cmd in cmds:
    i, o, e = c.exec_command(cmd)
    out = o.read().decode('utf-8')
    err = e.read().decode('utf-8')
    print(f"CMD: {cmd}")
    if out:
        print("OUT:", out)
    if err:
        print("ERR:", err)

c.close()
