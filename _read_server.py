import paramiko
c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect('198.13.184.39', username='root', password='Alcodome_99')

cmds = [
    'ls /tmp/hookah-mixes/',
    'ls /tmp/hookah-mixes/templates/',
    'ls /opt/hookah/',
    'cat /tmp/hookah-mixes/templates/index.html',
]
for cmd in cmds:
    print(f'\n=== {cmd} ===')
    i, o, e = c.exec_command(cmd)
    print(o.read().decode('utf-8'))
    err = e.read().decode('utf-8')
    if err:
        print('ERR:', err)
c.close()
