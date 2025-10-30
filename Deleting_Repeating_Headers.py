import pandas as pd
import os

for file in os.listdir('./Bronze'):
    df = pd.read_csv(os.path.join('./Bronze',file))
    df = df[~df.iloc[:,0].astype(str).isin(['Player', 'Date'])]
    df.to_csv(os.path.join('./Silver',file), index=False)