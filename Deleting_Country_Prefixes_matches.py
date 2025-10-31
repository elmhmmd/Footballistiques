import os
import pandas as pd
import re

pattern = re.compile(r'^[^A-Z]*(?=[A-Z])')

for filename in os.listdir('./Silver'):
    if filename.endswith("_matchlogs.csv"):
        path = os.path.join('./Silver', filename)

        df = pd.read_csv(path, header=None)

        df.iloc[:,9] = df.iloc[:,9].astype(str).apply(lambda x: pattern.sub('',x))

        df.to_csv(path, index=False, header=False)