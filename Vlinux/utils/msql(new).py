import pymysql
import pandas as pd
import logging
import os

# 获取当前脚本所在的目录
script_dir = os.path.dirname(os.path.abspath(__file__))
# 将日志文件路径设置为当前文件所在目录下的 app.log
log_file_path = os.path.join(script_dir, 'app.log')

# 配置日志
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class MySQLDatabase:
    # 初始化数据库连接，默认连接本地数据库，可以通过 host 参数指定远程数据库
    # location_type 参数用于指定数据库文件的位置，可选值为 'local' 和 'remote'
    def __init__(self, username, password, host='127.0.0.1', port=3306, location_type='local'):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.location_type = location_type
        self.connection = None
        self.cursor = None

        try:
            self.connection = pymysql.connect(host=self.host, user=self.username, password=self.password,
                                              port=self.port, cursorclass=pymysql.cursors.DictCursor)
            self.cursor = self.connection.cursor()
        except pymysql.Error as e:
            logging.error(f"Error occurred while connecting to the database: {e}")
            raise

    def create_user_and_grant_privileges(self, username, password, database='*', table='*', host='%'):
        try:
            self.cursor.execute(f"CREATE USER IF NOT EXISTS '{username}'@'{host}' IDENTIFIED BY '{password}'")
            self.cursor.execute(f"GRANT ALL PRIVILEGES ON {database}.{table} TO '{username}'@'{host}'")
            self.cursor.execute("FLUSH PRIVILEGES")
            logging.info(f"User '{username}' created and granted privileges successfully.")
        except pymysql.OperationalError as e:
            if "access denied" in str(e).lower():
                logging.error("The user used to create the cursor object does not have sufficient permissions. Please use a user with sufficient permissions.")
            else:
                logging.error(f"Error occurred while creating the user and granting privileges: {e}")
            raise
        except pymysql.Error as e:
            logging.error(f"Error occurred while creating the user and granting privileges: {e}")
            raise

    def create_database(self, database_name, charset='utf8mb4', collation='utf8mb4_unicode_ci'):
        try:
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name} CHARACTER SET {charset} COLLATE {collation}")
            logging.info(f"Database '{database_name}' created successfully.")
        except pymysql.Error as e:
            logging.error(f"Error occurred while creating the database: {e}")
            raise

    def delete_database(self, database_name):
        try:
            self.cursor.execute(f"DROP DATABASE IF EXISTS {database_name}")
            logging.info(f"Database '{database_name}' deleted successfully.")
        except pymysql.Error as e:
            logging.error(f"Error occurred while deleting the database: {e}")
            raise

    def create_table(self, database_name, table_name, fields):
        try:
            self.cursor.execute(f"USE {database_name}")
            field_definitions = ', '.join(fields)
            self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({field_definitions})")
            logging.info(f"Table '{table_name}' created successfully.")
        except pymysql.Error as e:
            logging.error(f"Error occurred while creating the table: {e}")
            raise

    def delete_table(self, database_name, table_name):
        try:
            self.cursor.execute(f"USE {database_name}")
            self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            logging.info(f"Table '{table_name}' deleted successfully.")
        except pymysql.Error as e:
            logging.error(f"Error occurred while deleting the table: {e}")
            raise

    def import_database(self, database_name, sql_file_path):
        if self.location_type == 'local':
            self.import_database_local(database_name, sql_file_path)
        elif self.location_type == 'remote':
            logging.error("Remote import not supported in refactored code.")
        else:
            logging.error(f"Invalid location_type: {self.location_type}")

    def export_database(self, database_name, sql_file_path):
        if self.location_type == 'local':
            self.export_database_local(database_name, sql_file_path)
        elif self.location_type == 'remote':
            logging.error("Remote export not supported in refactored code.")
        else:
            logging.error(f"Invalid location_type: {self.location_type}")

    def import_table(self, database_name, table_name, sql_file_path):
        if self.location_type == 'local':
            self.import_table_local(database_name, table_name, sql_file_path)
        elif self.location_type == 'remote':
            logging.error("Remote import not supported in refactored code.")
        else:
            logging.error(f"Invalid location_type: {self.location_type}")

    def export_table(self, database_name, table_name, sql_file_path):
        if self.location_type == 'local':
            self.export_table_local(database_name, table_name, sql_file_path)
        elif self.location_type == 'remote':
            logging.error("Remote export not supported in refactored code.")
        else:
            logging.error(f"Invalid location_type: {self.location_type}")

    def import_database_local(self, database_name, sql_file_path):
        try:
            with open(sql_file_path, 'r') as file:
                sql_statements = file.read()
                self.cursor.execute(sql_statements)
            logging.info(f"Database '{database_name}' imported successfully.")
        except FileNotFoundError as e:
            logging.error(f"Error occurred while importing the database: {e}")
            raise
        except pymysql.Error as e:
            logging.error(f"Error occurred while importing the database: {e}")
            raise

    def export_database_local(self, database_name, sql_file_path):
        try:
            with open(sql_file_path, 'w') as file:
                self.cursor.execute(f"USE {database_name}")
                result = self.cursor.fetchall()
                for row in result:
                    file.write(str(row))
                logging.info(f"Database '{database_name}' exported successfully.")
        except pymysql.Error as e:
            logging.error(f"Error occurred while exporting the database: {e}")
            raise

    def import_table_local(self, database_name, table_name, sql_file_path):
        try:
            with open(sql_file_path, 'r') as file:
                sql_statements = file.read()
                self.cursor.execute(sql_statements)
            logging.info(f"Table '{table_name}' in database '{database_name}' imported successfully.")
        except FileNotFoundError as e:
            logging.error(f"Error occurred while importing the table: {e}")
            raise
        except pymysql.Error as e:
            logging.error(f"Error occurred while importing the table: {e}")
            raise

    def export_table_local(self, database_name, table_name, sql_file_path):
        try:
            with open(sql_file_path, 'w') as file:
                self.cursor.execute(f"USE {database_name}")
                self.cursor.execute(f"SELECT * FROM {table_name}")
                result = self.cursor.fetchall()
                for row in result:
                    file.write(str(row))
                logging.info(f"Table '{table_name}' in database '{database_name}' exported successfully.")
        except pymysql.Error as e:
            logging.error(f"Error occurred while exporting the table: {e}")
            raise

    def insert_data(self, database_name, table_name, data, columns=None):
        try:
            self.cursor.execute(f"USE {database_name}")
            if columns:
                columns_str = ', '.join(columns)
                placeholders = ', '.join(['%s'] * len(columns))
                query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            else:
                placeholders = ', '.join(['%s'] * len(data[0]))
                query = f"INSERT INTO {table_name} VALUES ({placeholders})"
            self.cursor.executemany(query, data)
            self.connection.commit()
            logging.info("Data inserted successfully.")
        except pymysql.Error as e:
            self.connection.rollback()
            logging.error(f"Error occurred while inserting data: {e}")
            raise

    def select_data(self, database_name, table_name):
        try:
            self.cursor.execute(f"USE {database_name}")
            self.cursor.execute(f"SELECT * FROM {table_name}")
            results = self.cursor.fetchall()
            logging.info("Data selected successfully.")
            return results
        except pymysql.Error as e:
            logging.error(f"Error occurred while selecting data: {e}")
            raise

    def close_connection(self):
        if self.connection and self.cursor:
            self.cursor.close()
            self.connection.close()
            logging.info("Database connection closed.")
        else:
            logging.info("No connection to close.")

    def import_table_to_dataframe(self, database_name, table_name=None, query=None):
        try:
            self.cursor.execute(f"USE {database_name}")
            if query:
                logging.warning("Both table name and query provided. Ignoring the table name and using the provided query.")
                final_query = query
            elif table_name:
                final_query = f"SELECT * FROM {table_name}"
            else:
                logging.error("Either table name or query must be provided.")
                return None

            df = pd.read_sql(final_query, self.connection)
            logging.info("Data imported successfully.")
            return df
        except pymysql.Error as e:
            logging.error(f"Error occurred while importing data: {e}")
            return None

    def export_dataframe_to_table(self, dataframe, database_name, table_name=None):
        try:
            self.cursor.execute(f"USE {database_name}")
            if table_name:
                dataframe.to_sql(table_name, con=self.connection, if_exists='replace', index=False)
                logging.info(f"DataFrame data exported to table '{table_name}' successfully.")
            else:
                logging.error("Table name must be provided.")
        except pymysql.Error as e:
            logging.error(f"Error occurred while exporting DataFrame to table: {e}")
            raise


if __name__ == "__main__":
    db = MySQLDatabase(username="root", password="123456", host="127.0.0.1", port=3306, location_type='local')  
    try:       
        db.create_database("example_database", charset='utf8', collation='utf8_unicode_ci')      
        fields = ["id INT AUTO_INCREMENT PRIMARY KEY", "name VARCHAR(255)", "age INT"]
        db.create_table(database_name="example_database", table_name="example_table", fields=fields)
        data = [(None, 'Alice', 30), (None, 'Bob', 25)]
        columns = ['id', 'name', 'age']
        db.insert_data(database_name="example_database", table_name="example_table", data=data, columns=columns)
        db.select_data(database_name="example_database", table_name="example_table")  
        df = db.import_table_to_dataframe(database_name="example_database", table_name="example_table")
        db.export_dataframe_to_table(dataframe=df, database_name="example_database", table_name="new_table")
        db.delete_table(database_name="example_database", table_name="example_table")
        db.delete_database("example_database")

    except Exception as e:
        logging.error(f"An error occurred: {e}")

    finally:
        db.close_connection()
