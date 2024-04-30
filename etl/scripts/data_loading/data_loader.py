import mysql.connector

from etl.scripts.etl_config.etl_config import ETLConfig


class DataLoader:
    def __init__(self):
        self.host = ETLConfig.MYSQL_HOST
        self.user = ETLConfig.MYSQL_USER
        self.password = ETLConfig.MYSQL_PASSWORD
        self.database = ETLConfig.MYSQL_DATABASE2

    def load(self, data):
        try:
            conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if conn.is_connected():
                cursor = conn.cursor()

                query = "INSERT INTO output (date_, open_price, highest_price, lowest_price, close_price, volume) VALUES (%s, %s, %s, %s, %s, %s)"

                for row in data:
                    cursor.execute(query, row)
                    conn.commit()
                    print("Data loaded successfully")
        except mysql.connector.Error as error:
            print("Failed to connect with", error)

        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
