import pymysql
import pandas as pd
import paramiko
import subprocess
import logging
import os

# 获取当前文件所在的目录
current_directory = os.path.dirname(os.path.realpath(__file__))
# 将日志文件路径设置为当前文件所在目录下的 app.log
log_file_path = os.path.join(current_directory, 'app.log')

# 配置日志
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class SSHSingleton:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._ssh = None
        return cls._instance

    def connect(self, host, username, password=None, private_key_path=None):
        if password is None and private_key_path is None:
            logging.error("Either password or private_key_path must be provided.")
            raise ValueError("Either password or private_key_path must be provided.")

        if self._ssh is None or not self._ssh.get_transport().is_active():
            try:
                self._ssh = paramiko.SSHClient()
                self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                logging.info(f"Trying to connect to {host}...")
                if private_key_path:
                    private_key = paramiko.RSAKey.from_private_key_file(private_key_path)
                    self._ssh.connect(host, username=username, pkey=private_key)
                else:
                    self._ssh.connect(host, username=username, password=password)
                logging.info(f"Connected to {host}.")
            except paramiko.AuthenticationException as e:
                logging.error(f"Failed to authenticate with host {host}: {str(e)}")
                raise
            except paramiko.SSHException as e:
                logging.error(f"SSH connection to host {host} failed: {str(e)}")
                raise
            except Exception as e:
                logging.error(f"An unexpected error occurred: {str(e)}")
                raise

    def get_ssh(self):
        return self._ssh

    def close(self):
        if self._ssh is not None:
            self._ssh.close()
            self._ssh = None
            logging.info("SSH connection closed.")


class MySQLDatabase:
    # 初始化数据库连接, 默认连接本地数据库, 可以通过host参数指定远程数据库
    # location_type参数用于指定数据库文件的位置, 可选值为'local'和'remote'
    # 如果location_type为'remote', 则需要提供private_key_path参数
    def __init__(self, username, password, host='127.0.0.1', port='3306', private_key_path=None, location_type='local'):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.private_key_path = private_key_path
        self.location_type = location_type
        self.connection = None
        self.cursor = None
        self.engine = None
        self.ssh_singleton = SSHSingleton()

        try:
            self.connection = pymysql.connect(host=self.host, user=self.username, password=self.password,
                                              port=self.port, cursorclass=pymysql.cursors.DictCursor)
            self.cursor = self.connection.cursor()
            self.engine = self.connection  # 使用连接对象作为数据库引擎, 用于pandas的to_sql方法
        except pymysql.Error as e:
            logging.error(f"Error occurred while connecting to the database: {e}")
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
    #下面的方法的sql_file_path参数是指数据库文件的路径, 用于导入和导出数据库
    #特别注意: 如果是本地数据库, 则sql_file_path是本地文件的路径，
    #如果是远程数据库, 则sql_file_path是远程服务器上的文件路径，下面的其他方法也是一样！！！！
    def import_database(self, database_name, sql_file_path):
        if self.location_type == 'local':
            self.import_database_local(database_name, sql_file_path)
        elif self.location_type == 'remote':
            self.import_database_remote(database_name, sql_file_path)
        else:
            logging.error(f"Invalid location_type: {self.location_type}")

    def export_database(self, database_name, sql_file_path):
        if self.location_type == 'local':
            self.export_database_local(database_name, sql_file_path)
        elif self.location_type == 'remote':
            self.export_database_remote(database_name, sql_file_path)
        else:
            logging.error(f"Invalid location_type: {self.location_type}")

    def import_table(self, database_name, table_name, sql_file_path):
        if self.location_type == 'local':
            self.import_table_local(database_name, table_name, sql_file_path)
        elif self.location_type == 'remote':
            self.import_table_remote(database_name, table_name, sql_file_path)
        else:
            logging.error(f"Invalid location_type: {self.location_type}")

    def export_table(self, database_name, table_name, sql_file_path):
        if self.location_type == 'local':
            self.export_table_local(database_name, table_name, sql_file_path)
        elif self.location_type == 'remote':
            self.export_table_remote(database_name, table_name, sql_file_path)
        else:
            logging.error(f"Invalid location_type: {self.location_type}")

    def import_database_local(self, database_name, sql_file_path):
        try:
            command = f"mysql -u {self.username} -p{self.password} {database_name} < {sql_file_path}"
            subprocess.run(command, shell=True, check=True)
            logging.info(f"Database '{database_name}' imported successfully.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error occurred while importing the database: {e}")
            raise

    def export_database_local(self, database_name, sql_file_path):
        try:
            command = f"mysqldump -u {self.username} -p{self.password} {database_name} > {sql_file_path}"
            subprocess.run(command, shell=True, check=True)
            logging.info(f"Database '{database_name}' exported successfully.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error occurred while exporting the database: {e}")
            raise

    def import_table_local(self, database_name, table_name, sql_file_path):
        try:
            command = f"mysql -u {self.username} -p{self.password} {database_name} < {sql_file_path}"
            subprocess.run(command, shell=True, check=True)
            logging.info(f"Table '{table_name}' in database '{database_name}' imported successfully.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error occurred while importing the table: {e}")
            raise

    def export_table_local(self, database_name, table_name, sql_file_path):
        try:
            command = f"mysqldump -u {self.username} -p{self.password} {database_name} {table_name} > {sql_file_path}"
            subprocess.run(command, shell=True, check=True)
            logging.info(f"Table '{table_name}' in database '{database_name}' exported successfully.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error occurred while exporting the table: {e}")
            raise

    def import_database_remote(self, database_name, sql_file_path):
        try:
            ssh = self.ssh_singleton.get_ssh()
            command = f"mysql -u {self.username} -p{self.password} {database_name} < {sql_file_path}"
            stdin, stdout, stderr = ssh.exec_command(command)
            stdout.channel.recv_exit_status()
            logging.info(f"Database '{database_name}' imported successfully.")
        except paramiko.SSHException as e:
            logging.error(f"Error occurred while importing the database: {e}")
            raise

    def export_database_remote(self, database_name, sql_file_path):
        try:
            ssh = self.ssh_singleton.get_ssh()
            command = f"mysqldump -u {self.username} -p{self.password} {database_name} > {sql_file_path}"
            stdin, stdout, stderr = ssh.exec_command(command)
            stdout.channel.recv_exit_status()
            logging.info(f"Database '{database_name}' exported successfully.")
        except paramiko.SSHException as e:
            logging.error(f"Error occurred while exporting the database: {e}")
            raise

    def establish_ssh_connection(self):
        try:
            self.ssh_singleton.connect(self.remote_host, self.remote_user, password=self.remote_password, private_key_path=self.private_key_path)
        except Exception as e:
            logging.error(f"Error occurred while establishing SSH connection: {e}")
            raise

    def import_table_remote(self, database_name, table_name, sql_file_path):
        self.establish_ssh_connection()  # 建立SSH连接
        try:
            ssh = self.ssh_singleton.get_ssh()
            command = f"mysql -u {self.username} -p{self.password} {database_name} < {sql_file_path}"
            stdin, stdout, stderr = ssh.exec_command(command)
            stdout.channel.recv_exit_status()
            logging.info(f"Table '{table_name}' in database '{database_name}' imported successfully.")
        except paramiko.SSHException as e:
            logging.error(f"Error occurred while importing the table: {e}")
            raise

    def export_table_remote(self, database_name, table_name, sql_file_path):
        self.establish_ssh_connection()  # 建立SSH连接
        try:
            ssh = self.ssh_singleton.get_ssh()
            command = f"mysqldump -u {self.username} -p{self.password} {database_name} {table_name} > {sql_file_path}"
            stdin, stdout, stderr = ssh.exec_command(command)
            stdout.channel.recv_exit_status()
            logging.info(f"Table '{table_name}' in database '{database_name}' exported successfully.")
        except paramiko.SSHException as e:
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

            df = pd.read_sql(final_query, self.engine)
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
    db = MySQLDatabase(host="47.100.19.119",username="root", password="123456", port=3306, location_type='remote')  
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

