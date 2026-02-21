import pandas as pd
from pathlib import Path

rows = [
    ('002234','民和股份','2025-12-15'),
    ('603178','圣龙股份','2025-12-15'),
    ('301066','万事利','2025-12-18'),
    ('301082','久盛电气','2025-12-18'),
    ('003042','中农联合','2025-12-19'),
    ('301049','超越科技','2025-12-22'),
    ('002200','ST交投','2026-01-08'),
    ('301600','慧翰股份','2026-01-08'),
    ('000548','湖南投资','2026-01-22'),
    ('300051','琏升科技','2026-01-29'),
    ('603000','人民网','2026-01-29'),
    ('002648','卫星化学','2026-02-02'),
    ('300901','中胤时尚','2026-02-02'),
    ('002283','天润工业','2026-02-05'),
    ('000839','国安股份','2026-02-06'),
    ('300232','洲明科技','2026-02-11'),
    ('603507','振江股份','2026-02-11'),
    ('002282','博深股份','2026-02-12'),
    ('300217','东方电热','2026-02-12'),
    ('002952','亚世光电','2026-02-13'),
    ('300088','长信科技','2026-02-13'),
    ('603220','中贝通信','2026-02-13'),
    ('603721','ST天择','2026-02-13'),
    ('605006','山东玻纤','2026-02-13'),
]

df = pd.DataFrame(rows, columns=['stock_code','stock_name','signal_date'])

# optional csv copy in project outputs for traceability
out_dir = Path('outputs')
out_dir.mkdir(parents=True, exist_ok=True)
df.to_csv(out_dir / 'recent_2months_signals.csv', index=False, encoding='utf-8-sig')

# ths txt on desktop
desktop = Path(r'C:\Users\Administrator\Desktop')
txt_path = desktop / 'ths_watchlist_recent_2months_signal.txt'
codes = sorted(set(df['stock_code'].astype(str).str.zfill(6).tolist()))
txt_path.write_text('\n'.join(codes), encoding='utf-8')

print('TXT', txt_path)
print('COUNT', len(codes))
print('CSV', out_dir / 'recent_2months_signals.csv')
