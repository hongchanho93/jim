import pandas as pd
print(pd.read_csv('outputs/sample_signals_raw.csv').columns.tolist())
print(pd.read_csv('outputs/sample_signals_raw.csv').head(1).to_string(index=False))
