import pandas as pd
from pathlib import Path

out = Path('sample_data.parquet')
dates = pd.date_range('2020-01-03', periods=70, freq='W-FRI')
rows = []
for d in dates:
    rows.append({'date': d, 'code': 'sh.600000', 'open': 150.0, 'high': 151.0, 'low': 149.0, 'close': 150.0})
rows[10].update({'open': 45.0, 'high': 50.0, 'low': 40.0, 'close': 45.0})
rows[20].update({'open': 195.0, 'high': 220.0, 'low': 190.0, 'close': 200.0})
rows[30].update({'open': 105.0, 'high': 115.0, 'low': 100.0, 'close': 110.0})
pd.DataFrame(rows).to_parquet(out, index=False)
print(out)
