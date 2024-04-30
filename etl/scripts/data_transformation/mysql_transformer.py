class MySQLTransformer:
    @staticmethod
    def transform_data(data):
        transformed_data = []
        for row in data:
            row_list = list(row)
            transformed_row = row_list[:-1]
            transformed_data.append(tuple(transformed_row))
        return transformed_data
