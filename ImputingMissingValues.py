import pandas as pd
import os

for file in os.listdir('./Silver'):
    df = pd.read_csv(f'Silver/{file}')
    df.fillna(0,inplace=True)
    df.to_csv(f'Silver/{file}', index=False)