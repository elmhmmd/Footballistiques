import os
import pandas as pd

for filename in os.listdir('./Silver'):
    if filename.endswith("_matchlogs.csv"):
        path = os.path.join('./Silver', filename)

        df = pd.read_csv(path, header=None)

        for col in [7,8]:
            df.iloc[:, col] = df.iloc[:, col].astype(str).str.extract(r'(\d+)')[0].astype(int)

        df.to_csv(path, index=False, header=False)
