import paramiko

c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect('198.13.184.39', username='root', password='Alcodome_99', timeout=15)

print("=== CMD 1: curl -s http://localhost:8081/api/mixes | head -c 2000 ===")
stdin, stdout, stderr = c.exec_command('curl -s http://localhost:8081/api/mixes | head -c 2000')
print(stdout.read().decode())
err = stderr.read().decode()
if err:
    print("STDERR:", err)

print("\n=== CMD 2: journalctl -u hookah-mixes -n 20 --no-pager ===")
stdin, stdout, stderr = c.exec_command('journalctl -u hookah-mixes -n 20 --no-pager')
print(stdout.read().decode())
err = stderr.read().decode()
if err:
    print("STDERR:", err)

c.close()
