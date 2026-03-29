import paramiko
c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect('198.13.184.39', username='root', password='Alcodome_99')
stdin, stdout, stderr = c.exec_command('tail -n 50 /var/log/hookah-bot.log')
print(stdout.read().decode())
err = stderr.read().decode()
if err:
    print("STDERR:", err)
c.close()
