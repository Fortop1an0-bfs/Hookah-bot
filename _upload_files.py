import paramiko, sys
sys.stdout.reconfigure(encoding='utf-8')

c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect('198.13.184.39', username='root', password='Alcodome_99')

sftp = paramiko.SFTPClient.from_transport(c.get_transport())

# Upload updated files
sftp.put('C:/Project/Hookah/_new_index.html', '/opt/hookah-mixes/templates/index.html')
print("index.html uploaded")

sftp.put('C:/Project/Hookah/_new_main.py', '/opt/hookah-mixes/main.py')
print("main.py uploaded")

sftp.close()

# Restart the app
i, o, e = c.exec_command('cd /opt/hookah-mixes && /opt/hookah-mixes/venv/bin/python3 -m uvicorn main:app --host 0.0.0.0 --port 8081 --reload & sleep 2 && echo "Restarted"')
# Actually use pkill + start
i2, o2, e2 = c.exec_command('pkill -f "uvicorn main:app" ; sleep 1 ; cd /opt/hookah-mixes && nohup /opt/hookah-mixes/venv/bin/python3 -m uvicorn main:app --host 0.0.0.0 --port 8081 > /var/log/hookah-mixes.log 2>&1 &')
import time; time.sleep(3)
i3, o3, e3 = c.exec_command('ps aux | grep uvicorn | grep -v grep')
print("Process check:", o3.read().decode('utf-8', errors='replace').strip())

c.close()
print("Done!")
