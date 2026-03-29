import paramiko, sys
sys.stdout.reconfigure(encoding='utf-8')

c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect('198.13.184.39', username='root', password='Alcodome_99')

cmds = [
    'ls /opt/hookah-mixes/',
    'ls /opt/hookah-mixes/templates/',
    'cat /opt/hookah-mixes/templates/index.html',
    'cat /opt/hookah-mixes/main.py',
]

for cmd in cmds:
    print(f'\n=== {cmd} ===')
    i, o, e = c.exec_command(cmd)
    print(o.read().decode('utf-8', errors='replace'))
    err = e.read().decode('utf-8', errors='replace')
    if err: print('ERR:', err)

c.close()
