import paramiko

HOST = "198.13.184.39"
USER = "root"
PASS = "Alcodome_99"
LOCAL  = r"C:\Project\Hookah\bot\agents\research_agent.py"
REMOTE = "/opt/hookah/bot/agents/research_agent.py"

# --- SFTP Upload ---
try:
    transport = paramiko.Transport((HOST, 22))
    transport.connect(username=USER, password=PASS)
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.put(LOCAL, REMOTE)
    sftp.close()
    transport.close()
    print("UPLOAD_OK")
except Exception as e:
    print(f"UPLOAD_FAIL: {e}")
    raise SystemExit(1)

# --- SSH: restart service and check status ---
try:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, username=USER, password=PASS)

    cmds = "systemctl restart hookah-bot && sleep 3 && systemctl status hookah-bot --no-pager"
    stdin, stdout, stderr = ssh.exec_command(cmds, timeout=30)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    ssh.close()

    import sys
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    print("=== SERVICE STATUS ===")
    print(out)
    if err:
        print("STDERR:", err)
except Exception as e:
    print(f"SSH_FAIL: {e}")
