import mysql.connector

from etl.scripts.etl_config.etl_config import ETLConfig


class MySQLExtractor:
    def __init__(self):
        self.host = ETLConfig.MYSQL_HOST
        self.user = ETLConfig.MYSQL_USER
        self.password = ETLConfig.MYSQL_PASSWORD
        self.database = ETLConfig.MYSQL_DATABASE1

    def extract(self):
        try:
            conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if conn.is_connected():
                cursor = conn.cursor()

                cursor.execute("SELECT * FROM bitcoin")

                rows = cursor.fetchall()

                return rows
        except mysql.connector.Error as error:
            print("Failed to connect with", error)
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
