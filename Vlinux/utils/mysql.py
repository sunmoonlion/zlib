import logging
import os
import subprocess
import pandas as pd
from sqlalchemy import create_engine, text, Table, Column, Integer, String, MetaData
from sqlalchemy.orm import sessionmaker
from file import FileTransfer, SSHSingleton

class MySQLDatabase:
    def __init__(self, username, password, host='127.0.0.1', port=3306,
                 location_type='local',
                 remote_host=None, remote_user=None, remote_password=None, private_key_path=None,
                 local_mysql_path='/home/WUYING_13701819268_15611880/anaconda3/bin/mysql', local_mysqldump_path='/home/WUYING_13701819268_15611880/anaconda3/bin/mysqldump',
                 remote_mysql_path='/home/zym/anaconda3/bin/mysql', remote_mysqldump_path='/home/zym/anaconda3/bin/mysqldump'):
        
         # 定义数据库连接参数，包括用户名、密码、主机地址和端口
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        
        # 定义是否是远程连接的标志，如果是远程连接，则需要提供远程主机的相关信息
        # 远程连接只有在导入和导出数据库时才需要，其他操作都不需要：如果数据库在本地，则host为本地IP地址，否则为远程IP地址，但是location_type都是local，因为数据库既可以在本地也可以在远程操作
        self.location_type = location_type
        # 定义远程主机的相关信息
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.remote_password = remote_password
        # 定义远程主机的私钥路径,密钥和密码二选一
        self.private_key_path = private_key_path
        # 定义SSH单例对象
        self.ssh_singleton = SSHSingleton()
        # MySQL 和 mysqldump 命令的绝对路径
        self.local_mysql_path = local_mysql_path
        self.local_mysqldump_path = local_mysqldump_path
        self.remote_mysql_path = remote_mysql_path
        self.remote_mysqldump_path = remote_mysqldump_path      
              
        try:
            self.engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/")
        except SQLAlchemyError as e:
            logging.error(f"Error occurred while connecting to the database: {e}")
            raise

    def _establish_ssh_connection(self):
        self.ssh_singleton.connect(self.remote_host, self.remote_user, password=self.remote_password, private_key_path=self.private_key_path)

    def execute_ssh_command(self, command):
        print("Executing SSH command:", command)
        self._establish_ssh_connection()
        ssh = self.ssh_singleton.get_ssh()
        stdin, stdout, stderr = ssh.exec_command(command)
        stdin = None
        output = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')
        return stdin, output, error

    def get_transfer(self):
        transfer = FileTransfer(remote_host=self.host, remote_user=self.username, remote_password=self.password,
                                private_key_path=self.private_key_path)
        return transfer

    def create_user_and_grant_privileges(self, new_user, new_user_password, pri_database='*', pri_table='*', pri_host='%'):
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE USER IF NOT EXISTS '{new_user}'@'{pri_host}' IDENTIFIED BY '{new_user_password}'"))
                conn.execute(text(f"GRANT ALL PRIVILEGES ON {pri_database}.{pri_table} TO '{new_user}'@'{pri_host}'"))
                conn.execute(text("FLUSH PRIVILEGES"))
            logging.info(f"User '{new_user}' created and granted privileges successfully.")
        except Exception as e:
            logging.error(f"Error occurred while creating the user and granting privileges: {e}")
            raise

    def create_database(self, database_name, charset='utf8mb4', collation='utf8mb4_unicode_ci'):
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {database_name} CHARACTER SET {charset} COLLATE {collation}"))
            logging.info(f"Database '{database_name}' created successfully.")
        except Exception as e:
            logging.error(f"Error occurred while creating the database: {e}")
            raise

    def delete_database(self, database_name):
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"DROP DATABASE IF EXISTS {database_name}"))
            logging.info(f"Database '{database_name}' deleted successfully.")
        except Exception as e:
            logging.error(f"Error occurred while deleting the database: {e}")
            raise

    # 使用SQLAlchemy创建表，而且直接使用SQL语句，要注意的是，后续与orm结合使用时，要注意表的定义，特别要注意字段的类型要和orm的类型一致      
    def create_table(self, database_name, table_name, fields):
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"USE {database_name}"))
                field_definitions = ', '.join(fields)
                conn.execute(text(f"CREATE TABLE IF NOT EXISTS {table_name} ({field_definitions})"))
                logging.info(f"Table '{table_name}' created successfully.")
        except SQLAlchemyError as e:
            logging.error(f"Error occurred while creating the table: {e}")
            raise
    
    # 使用SQLAlchemy删除表，而且直接使用SQL语句，要注意的是，后续与orm结合使用时，要注意表的定义，特别要注意字段的类型要和orm的类型一致
    def delete_table(self, database_name, table_name):
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"USE {database_name}"))
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                logging.info(f"Table '{table_name}' deleted successfully.")
        except SQLAlchemyError as e:
            logging.error(f"Error occurred while deleting the table: {e}")
            raise

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

    def import_table(self, database_name, tablename, sql_file_path):
        if self.location_type == 'local':
            self.import_table_local(database_name, tablename, sql_file_path)
        elif self.location_type == 'remote':
            self.import_table_remote(database_name, tablename, sql_file_path)
        else:
            logging.error(f"Invalid location_type: {self.location_type}")

    def export_table(self, database_name, tablename, sql_file_path):
        if self.location_type == 'local':
            self.export_table_local(database_name, tablename, sql_file_path)
        elif self.location_type == 'remote':
            self.export_table_remote(database_name, tablename, sql_file_path)
        else:
            logging.error(f"Invalid location_type: {self.location_type}")

    def export_database_local(self, database_name, sql_file_path):
        try:
            command = f"{self.local_mysqldump_path} -u {self.username} -p{self.password} -h {self.host} --port={self.port} {database_name} > {sql_file_path}"
            subprocess.run(command, shell=True, check=True)
            logging.info(f"Database '{database_name}' exported successfully.")
        except Exception as e:
            logging.error(f"Error occurred while exporting the database: {e}")
            raise

    def export_table_local(self, database_name, table_name, sql_file_path):
        try:
            command = f"{self.local_mysqldump_path} -u {self.username} -p{self.password} -h {self.host} --port={self.port} {database_name} {table_name} > {sql_file_path}"
            subprocess.run(command, shell=True, check=True)
            logging.info(f"Table '{table_name}' in database '{database_name}' exported successfully.")
        except Exception as e:
            logging.error(f"Error occurred while exporting the table: {e}")
            raise

    def import_database_local(self, database_name, sql_file_path):
        try:
            command = f"{self.local_mysql_path} -u {self.username} -p{self.password} -h {self.host} --port={self.port} {database_name} < {sql_file_path}"
            subprocess.run(command, shell=True, check=True)
            logging.info(f"Database '{database_name}' imported successfully.")
        except Exception as e:
            logging.error(f"Error occurred while importing the database: {e}")
            raise

    def import_table_local(self, database_name, table_name, sql_file_path):
        try:
            command = f"{self.local_mysql_path} -u {self.username} -p{self.password} -h {self.host} --port={self.port} {database_name} < {sql_file_path}"
            subprocess.run(command, shell=True, check=True)
            logging.info(f"Table '{table_name}' in database '{database_name}' imported successfully.")
        except Exception as e:
            logging.error(f"Error occurred while importing the table: {e}")
            raise

    def import_database_remote(self, database_name, sql_file_path):
        try:
            command = f"if [ -f {sql_file_path} ]; then echo 'true'; else echo 'false'; fi"
            _, stdout, stderr = self.execute_ssh_command(command)
            if stdout.strip() == 'true':
                logging.info(f"Importing existing remote SQL file: {sql_file_path} to database: {database_name}")
                command = f"{self.remote_mysql_path} -u {self.username} -p{self.password} -h {self.host} --port={self.port} {database_name} < {sql_file_path}"
                self.execute_ssh_command(command)
            else:
                logging.info(f"SQL file: {sql_file_path} does not exist remotely, uploading from local.")
                transfer = self.get_transfer()
                transfer.upload(sql_file_path, sql_file_path)
                command = f"{self.remote_mysql_path} -u {self.username} -p{self.password} -h {self.host} --port={self.port} {database_name} < {sql_file_path}"
                self.execute_ssh_command(command)
            logging.info(f"Database '{database_name}' imported successfully from remote SQL file.")
        except Exception as e:
            logging.error(f"Error occurred while importing the database from remote: {e}")
            raise

    def export_database_remote(self, database_name, sql_file_path):
        try:
            command = f"{self.remote_mysqldump_path} -u {self.username} -p{self.password} -h {self.host} --port={self.port} {database_name} > {sql_file_path}"
            self.execute_ssh_command(command)
            transfer = self.get_transfer()
            transfer.download(sql_file_path, sql_file_path)
            logging.info(f"Database '{database_name}' exported successfully to remote SQL file.")
        except Exception as e:
            logging.error(f"Error occurred while exporting the database to remote: {e}")
            raise

    def import_table_remote(self, database_name, table_name, sql_file_path):
        try:
            command = f"if [ -f {sql_file_path} ]; then echo 'true'; else echo 'false'; fi"
            _, stdout, stderr = self.execute_ssh_command(command)
            if stdout.strip() == 'true':
                logging.info(f"Importing existing remote SQL file: {sql_file_path} to table: {table_name} in database: {database_name}")
                command = f"{self.remote_mysql_path} -u {self.username} -p{self.password} -h {self.host} --port={self.port} {database_name} < {sql_file_path}"
                self.execute_ssh_command(command)
            else:
                logging.info(f"SQL file: {sql_file_path} does not exist remotely, uploading from local.")
                transfer = self.get_transfer()
                transfer.upload(sql_file_path, sql_file_path)
                command = f"{self.remote_mysql_path} -u {self.username} -p{self.password} -h {self.host} --port={self.port} {database_name} < {sql_file_path}"
                self.execute_ssh_command(command)
            logging.info(f"Table '{table_name}' in database '{database_name}' imported successfully from remote SQL file.")
        except Exception as e:
            logging.error(f"Error occurred while importing the table from remote: {e}")
            raise

    def export_table_remote(self, database_name, table_name, sql_file_path):
        try:
            command = f"{self.remote_mysqldump_path} -u {self.username} -p{self.password} -h {self.host} --port={self.port} {database_name} {table_name} > {sql_file_path}"
            self.execute_ssh_command(command)
            transfer = self.get_transfer()
            transfer.download(sql_file_path, sql_file_path)
            logging.info(f"Table '{table_name}' in database '{database_name}' exported successfully to remote SQL file.")
        except Exception as e:
            logging.error(f"Error occurred while exporting the table to remote: {e}")
            raise

    def import_table_to_dataframe(self, database_name, table_name=None, query=None):
        try:
            if query:
                logging.warning("Both table name and query provided. Ignoring the table name and using the provided query.")
                final_query = query
            elif table_name:
                final_query = f"SELECT * FROM {table_name}"
            else:
                logging.error("Either table name or query must be provided.")
                return None

            with self.engine.connect() as conn:
                df = pd.read_sql(final_query, conn, database_name)
            logging.info("Data imported successfully.")
            return df
        except Exception as e:
            logging.error(f"Error occurred while importing data: {e}")
            return None

    def export_dataframe_to_table(self, dataframe, database_name, table_name=None):
        try:
            if table_name:
                with self.engine.connect() as conn:
                    dataframe.to_sql(table_name, con=conn, if_exists='replace', index=False)
                logging.info(f"DataFrame data exported to table '{table_name}' successfully.")
            else:
                logging.error("Table name must be provided.")
        except Exception as e:
            logging.error(f"Error occurred while exporting DataFrame to table: {e}")
            raise
