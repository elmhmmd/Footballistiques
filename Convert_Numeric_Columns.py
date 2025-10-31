import os
import pandas as pd

folder = "./Silver"

for filename in os.listdir(folder):
    if filename.endswith("_matchlogs.csv"):
        path = os.path.join(folder, filename)
        df = pd.read_csv(path)

        for i in [10, 11, 12]:
            df.iloc[:, i] = pd.to_numeric(df.iloc[:, i])

        df.to_csv(path, index=False)


