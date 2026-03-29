import paramiko
import sys

sys.stdout.reconfigure(encoding='utf-8')

c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect('198.13.184.39', username='root', password='Alcodome_99')
i, o, e = c.exec_command('tail -n 100 /var/log/hookah-bot.log')
output = o.read().decode('utf-8')
print(output)
err = e.read().decode('utf-8')
if err:
    print("STDERR:", err)
c.close()
