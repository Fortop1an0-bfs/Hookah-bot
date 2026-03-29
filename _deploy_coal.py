import paramiko, sys
sys.stdout.reconfigure(encoding='utf-8')

c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect('198.13.184.39', username='root', password='Alcodome_99')

# 1. Add coal_tip column
migration = """
PGPASSWORD=hookah123 psql -U hookah -h 127.0.0.1 -d hookah_db -c "
ALTER TABLE web_mixes ADD COLUMN IF NOT EXISTS coal_tip text;
"
"""
i, o, e = c.exec_command(migration)
print("Migration:", o.read().decode('utf-8', errors='replace'))
err = e.read().decode('utf-8', errors='replace')
if err: print("ERR:", err)

# 2. Update coal_tip per mix
# Based on tobacco analysis:
# DS Core/Torpedo = high heat -> 3 coals 5-6 min
# Хулиган HARD + убивашка = high -> 3 coals 5-6 min
# DS Base / Хулиган Base / MustHave / BB = medium -> 3 coals 4-5 min (убивашка) or 2-3 (фанел)
# MustHave dominant, фанел = light -> 2 coals 3-4 min

coal_tips = {
    1:  "Прогрев: 3 угля, 5 мин с колпаком → Курить: 3 угля с колпаком",       # Ягодный лимонад (DS Core + убивашка)
    2:  "Прогрев: 3 угля, 4–5 мин с колпаком → Курить: 2–3 угля с колпаком",   # Лесная поляна (DS Base + убивашка)
    3:  "Прогрев: 2 угля, 3–4 мин с колпаком → Курить: 2 угля с колпаком",     # Ягодный сорбет (BB+MH, фанел)
    4:  "Прогрев: 3 угля, 4–5 мин с колпаком → Курить: 2–3 угля с колпаком",   # Тропический бум (DS Base + убивашка)
    5:  "Прогрев: 3 угля, 5 мин с колпаком → Курить: 3 угля без колпака",      # Бали бриз (Хулиган HARD, крепкий)
    6:  "Прогрев: 3 угля, 4–5 мин с колпаком → Курить: 2–3 угля с колпаком",   # Ананасовый панч (убивашка)
    7:  "Прогрев: 3 угля, 5 мин с колпаком → Курить: 3 угля без колпака",      # Виноградный огурец (HARD, крепкий)
    8:  "Прогрев: 2 угля, 3–4 мин с колпаком → Курить: 2 угля с колпаком",     # Зелёный фреш (MH dominant, фанел)
    9:  "Прогрев: 3 угля, 5–6 мин с колпаком → Курить: 3 угля без колпака",    # Вишнёвая кола (HARD + убивашка, крепкий)
    10: "Прогрев: 3 угля, 5 мин с колпаком → Курить: 3 угля с колпаком",       # Виноградный лимонад (DS Core + убивашка)
    11: "Прогрев: 2–3 угля, 4 мин с колпаком → Курить: 2 угля с колпаком",     # Манговый чизкейк (фанел)
    12: "Прогрев: 2 угля, 3 мин с колпаком → Курить: 2 угля с колпаком",       # Клубничный десерт (MH x3, лёгкий)
    13: "Прогрев: 3 угля, 4–5 мин с колпаком → Курить: 2–3 угля с колпаком",   # Апельсиновый шок (убивашка)
    14: "Прогрев: 2 угля, 3–4 мин с колпаком → Курить: 2 угля с колпаком",     # Грейп + яблоко (фанел, лёгкий)
    15: "Прогрев: 3 угля, 5–6 мин с колпаком → Курить: 3 угля с колпаком",     # Торпедо микс (DS Torpedo/Core)
    16: "Прогрев: 3 угля, 5–6 мин с колпаком → Курить: 3 угля без колпака",    # Дынный тропик (DS Torpedo + HARD)
}

for mix_id, tip in coal_tips.items():
    safe_tip = tip.replace("'", "''")
    cmd = f"PGPASSWORD=hookah123 psql -U hookah -h 127.0.0.1 -d hookah_db -c \"UPDATE web_mixes SET coal_tip='{safe_tip}' WHERE id={mix_id};\""
    i, o, e = c.exec_command(cmd)
    out = o.read().decode('utf-8', errors='replace').strip()
    err_out = e.read().decode('utf-8', errors='replace').strip()
    if 'UPDATE 1' in out:
        print(f"  [{mix_id}] OK")
    else:
        print(f"  [{mix_id}] {out} {err_out}")

print("\nDone. Verifying:")
i, o, e = c.exec_command("PGPASSWORD=hookah123 psql -U hookah -h 127.0.0.1 -d hookah_db -c \"SELECT id, name, coal_tip FROM web_mixes ORDER BY id;\"")
print(o.read().decode('utf-8', errors='replace'))

c.close()
