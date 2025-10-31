import os
import pandas as pd
import re

BRONZE_PATH = './Bronze'
SILVER_PATH = './Silver'

for filename in os.listdir(BRONZE_PATH):
    bronze_file_path = os.path.join(BRONZE_PATH, filename)
    silver_file_path = os.path.join(SILVER_PATH, filename)

    if filename.endswith("_matchlogs.csv"):
        print(f"Processing matchlog file: {filename}")
        
        matchlog_cols = [
            'Date', 'Time', 'Comp', 'Round', 'Day', 'Venue', 'Result', 'GF', 'GA',
            'Opponent', 'xG', 'xGA', 'Poss', 'Attendance', 'Captain', 'Formation',
            'Opponent Formation', 'Referee'
        ]
        
        df = pd.read_csv(bronze_file_path, header=None, names=matchlog_cols)

        df = df[df.iloc[:, 0].astype(str) != 'Date']

 
        df.iloc[:, 7] = df.iloc[:, 7].astype(str).str.extract(r'(\d+)')[0] 
        df.iloc[:, 8] = df.iloc[:, 8].astype(str).str.extract(r'(\d+)')[0]

        df.iloc[:, 9] = df.iloc[:, 9].astype(str).apply(lambda x: re.sub(r'^[a-z]{2}\s', '', x))

        df.iloc[:, 1] = df.iloc[:, 1].astype(str).str.split(' ').str[0]

        df.iloc[:, 13] = df.iloc[:, 13].astype(str).str.replace(',', '', regex=False)

        numeric_indices = [7, 8, 10, 11, 12, 13]
        for i in numeric_indices:
            df.iloc[:, i] = pd.to_numeric(df.iloc[:, i], errors='coerce')
        
        for i in numeric_indices:
            df.iloc[:, i] = df.iloc[:, i].fillna(0)

        for i in [7, 8, 13]: 
            df.iloc[:, i] = df.iloc[:, i].astype(int)

        df.to_csv(silver_file_path, index=False)

    elif filename.endswith("_standard.csv"):
        print(f"Processing standard stats file: {filename}")

        standard_cols = [
            'Player', 'Nation', 'Pos', 'Age', 'MP', 'Starts', 'Min', '90s', 'Gls',
            'Ast', 'G+A', 'G-PK', 'PK', 'PKatt', 'CrdY', 'CrdR'
        ]
        
        df = pd.read_csv(bronze_file_path, header=None, names=standard_cols)
        
        df = df[df.iloc[:, 0].astype(str) != 'Player']
        df = df.dropna(subset=['Min'])
        df = df[df['Min'] != '']
        
        df.iloc[:, 1] = df.iloc[:, 1].astype(str).str.split(' ').str[-1]

        df.iloc[:, 6] = df.iloc[:, 6].astype(str).str.replace(',', '', regex=False)

        numeric_indices = list(range(3, 16))
        for i in numeric_indices:
            df.iloc[:, i] = pd.to_numeric(df.iloc[:, i], errors='coerce')

        df.iloc[:, 3:16] = df.iloc[:, 3:16].fillna(0)


        int_indices = [3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15]
        for i in int_indices:
            df.iloc[:, i] = df.iloc[:, i].astype(int)

        df.to_csv(silver_file_path, index=False)

print("\n--- Unified cleaning process complete. All files processed and saved to Silver folder. ---")