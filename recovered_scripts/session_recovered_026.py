import pandas as pd
from pathlib import Path

src = Path('outputs/signal_code_name_date_2025.xlsx')
desktop = Path(r'C:\Users\Administrator\Desktop')
out = desktop / 'ths_watchlist_2025_signal.txt'

df = pd.read_excel(src, sheet_name='signals', dtype={'stock_code': str})
codes = (
    df['stock_code']
    .dropna()
    .astype(str)
    .str.extract(r'(\d+)')[0]
    .dropna()
    .str.zfill(6)
)
uniq = sorted(set(codes.tolist()))

out.write_text('\n'.join(uniq), encoding='utf-8')
print('OUT_FILE', out)
print('COUNT', len(uniq))
