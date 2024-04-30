import os
import unittest

import pandas as pd

from etl.scripts.data_extraction.csv_extractor import CSVExtractor


class TestCSVExtractor(unittest.TestCase):
    def test_extract(self):
        # Create a temporary directory and some CSV files for testing
        csv_data = [
            {'Name': 'Alice', 'Age': 30, 'City': 'New York'},
            {'Name': 'Bob', 'Age': 25, 'City': 'San Francisco'}
        ]
        csv_file_path = 'temp_csv_files'
        os.makedirs(csv_file_path, exist_ok=True)
        for i, data in enumerate(csv_data):
            data_df = pd.DataFrame(data, index=[0])
            data_df.to_csv(os.path.join(csv_file_path, f'test_data_{i}.csv'), index=False)

        # Instantiate CSVExtractor with the temporary directory
        csv_extractor = CSVExtractor(csv_file_path)

        # Extract data
        extracted_data = csv_extractor.extract()

        # Assert the extracted data is correct
        expected_data = pd.DataFrame(csv_data)
        pd.testing.assert_frame_equal(extracted_data, expected_data)

        # Clean up temporary directory
        for file in os.listdir(csv_file_path):
            file_path = os.path.join(csv_file_path, file)
            os.remove(file_path)
        os.rmdir(csv_file_path)


if __name__ == '__main__':
    unittest.main()
