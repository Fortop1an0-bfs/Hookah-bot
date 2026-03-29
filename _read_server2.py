import paramiko
c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect('198.13.184.39', username='root', password='Alcodome_99')

cmds = [
    'cat /tmp/hookah-mixes/main.py',
    'pm2 list',
    'ls /opt/hookah/bot/',
]
for cmd in cmds:
    print(f'\n=== {cmd} ===')
    i, o, e = c.exec_command(cmd)
    print(o.read().decode('utf-8', errors='replace'))
    err = e.read().decode('utf-8', errors='replace')
    if err:
        print('ERR:', err)
c.close()
