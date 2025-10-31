import os
import pandas as pd
import re

pattern = re.compile(r'^[^A-Z]*(?=[A-Z])')

for filename in os.listdir('./Silver'):
    if filename.endswith("_standard.csv"):
        path = os.path.join('./Silver', filename)

        df = pd.read_csv(path, header=None)

        df.iloc[:,1] = df.iloc[:,1].astype(str).apply(lambda x: pattern.sub('',x))

        df.to_csv(path, index=False, header=False)