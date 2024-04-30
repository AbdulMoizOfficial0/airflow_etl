class CSVTransformer:
    @staticmethod
    def remove_last_column(df):
        return df.iloc[:, :-1]
