import os

import pandas as pd


class CSVExtractor:
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path

    def extract(self):
        csv_files = [file for file in os.listdir(self.csv_file_path) if file.endswith('.csv')]
        df = []
        for file in csv_files:
            file_path = os.path.join(self.csv_file_path, file)
            data = pd.read_csv(file_path)
            df.append(data)
        return pd.concat(df, ignore_index=True)
