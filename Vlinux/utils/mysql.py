import logging
import os
import re
import pandas as pd
import subprocess
import pymysql
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from file import FileTransfer, SSHSingleton
from container.container import Container
from time import sleep
# 本数据库导入时，如果不想导入特定的数据库或表，那么就可以用iimport_all_databases_from_sql_file或export_all_databases_from_sql_file把所有的数据库导入导出（数据库文件中要有创建数据库的mysql语句）
# 只有想导入导出特定的数据库，才考虑用import_database,export_database（要指定数据库，导入的数据库文件要包含创建数据库的mysql语句）,import_table.export_tabl（要使用指定的数据库，要包含指定的表，数据库文件中要有表创建mysql语句）函数
class MySQLDatabase:
    def __init__(self, mysqlusername=None, mysqlpassword=None, mysqlhost='127.0.0.1', mysqlport=3306,max_attempts=None,sleep_time=None,
                 container_is_up=True,
                 location_type='local',
                 local_path=None,remote_path=None,service_name=None,remote_user=None,remote_host=None,private_key_path=None,remote_password=None,
                 local_mysql_path=None, local_mysqldump_path=None,remote_mysql_path=None, remote_mysqldump_path= None):
        
        # 定义数据库连接参数，包括数据库的用户名、数据库的密码、数据库所在主机地址和端口
        # 注意数据库的主机地址和端口，既可以是本地，也可以是远程
        self.mysqlusername = mysqlusername
        self.mysqlpassword = mysqlpassword
        self.mysqlhost = mysqlhost
        self.mysqlport = mysqlport
        #定义试错次数和时间，管理主数据库需要，创建容器也需要
        self.max_attempts = max_attempts
        self.sleep_time = sleep_time 
        
        #定义数据库容器是否开启
        self.container_is_up = container_is_up
        # 定义是否是远程连接的标志，如果是远程连接，则需要提供远程主机的相关信息
         # 定义要启动的服务（容器）
        self.service_name = service_name  
        #定义要导入的数据库是在本地还是远程
        self.location_type = location_type
        
        # 定义创建容器的初始化参数
        #定义创建容器的yaml文件及其相关文件的地址
        self.local_path = local_path
        self.remote_path = remote_path  
        # 定义远程连接时所需要的主机地址，用户名和密钥或密码
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.private_key_path = private_key_path
        self.remote_password = remote_password
        # MySQL 和 mysqldump 命令的绝对路径，以便对数据库进行导入和导出操作
        self.local_mysql_path = local_mysql_path
        self.local_mysqldump_path = local_mysqldump_path
        self.remote_mysql_path = remote_mysql_path
        self.remote_mysqldump_path = remote_mysqldump_path 
        
        # 定义SSH单例对象
        self.ssh_singleton = SSHSingleton()
        
        
        
        self.up_container()  
        
        self.engine=self.create_engine_with_retries()
        self.connection=self.create_connection_with_retries ()     
        
    def create_engine_with_retries(self):
        attempts = 0
        while attempts < self.max_attempts:
            try:
                engine = create_engine(f"mysql+pymysql://{self.mysqlusername}:{self.mysqlpassword}@{self.mysqlhost}:{self.mysqlport}/")
                print("Engine created successfully.")
                return engine
            except SQLAlchemyError as e:
                logging.error(f"Error occurred while connecting to the database (SQLAlchemy): {e}")
                attempts += 1
                if attempts < self.max_attempts:
                    print(f"Retrying to connect to the database engine in {self.sleep_time} seconds...")
                    sleep(self.sleep_time)
                else:
                    raise

    def create_connection_with_retries(self):
        attempts = 0
        while attempts < self.max_attempts:
            try:
                connection = pymysql.connect(user=self.mysqlusername, password=self.mysqlpassword, host=self.mysqlhost, port=self.mysqlport)
                print("Connection established successfully.")
                return connection
            except pymysql.MySQLError as e:
                logging.error(f"Error occurred while connecting to the database (pymysql): {e}")
                attempts += 1
                if attempts < self.max_attempts:
                    print(f"Retrying to connect to the database in {self.sleep_time} seconds...")
                    sleep(self.sleep_time)
                else:
                    raise

        
        
    def up_container(self):
        if not self.container_is_up:
            try:
                container = Container(self.local_path, self.remote_path, self.location_type, self.service_name,
                                      self.remote_host, self.remote_user,self.private_key_path, self.remote_password,
                                      self.max_attempts, self.sleep_time)
                container.up_services()
            except Exception as e:
                logging.error(f"Error occurred while starting the container: {e}")
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
        transfer = FileTransfer(remote_host=self.remote_host, remote_user=self.remote_user, remote_password=self.remote_password,
                                private_key_path=self.private_key_path)
        return transfer

    def create_user_and_grant_privileges(self, new_user, new_user_password, pri_database='*', pri_table='*', pri_host='%'):
        try:
            with self.engine.connect() as conn:
                # 使用正确的字符串拼接方式，确保 pri_host 中的 '%' 不会被错误处理
                create_user_sql = f"CREATE USER IF NOT EXISTS '{new_user}'@'{pri_host}' IDENTIFIED BY '{new_user_password}'"
                grant_privileges_sql = f"GRANT ALL PRIVILEGES ON {pri_database}.{pri_table} TO '{new_user}'@'{pri_host}'"
                
                # 打印调试信息
                print(f"Executing SQL: {create_user_sql}")
                print(f"Executing SQL: {grant_privileges_sql}")

                conn.execute(text(create_user_sql))
                conn.execute(text(grant_privileges_sql))
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

    def delete_table(self, database_name, table_name):
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"USE {database_name}"))
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            logging.info(f"Table '{table_name}' deleted successfully.")
        except SQLAlchemyError as e:
            logging.error(f"Error occurred while deleting the table: {e}")
            raise

    # 如果指定某库，那么，sql_file_path文件中的所有库和表都会被导入
    def import_database(self, database_name, sql_file_path):
        if self.location_type == 'local':
            self.import_database_local(database_name, sql_file_path)
        elif self.location_type == 'remote':
            self.import_database_remote(database_name, sql_file_path)
        else:
            logging.error(f"Invalid location_type: {self.location_type}")


    def import_database_local(self, database_name, sql_file_path):
        try:
            command = f"{self.local_mysql_path} -u {self.mysqlusername} -p{self.mysqlpassword} -h {self.mysqlhost} --port={self.mysqlport} {database_name} < {sql_file_path}"
            subprocess.run(command, shell=True, check=True)
            logging.info(f"Database '{database_name}' imported successfully.")
        except Exception as e:
            logging.error(f"Error occurred while importing the database: {e}")
            raise

    def import_database_remote(self, database_name, sql_file_path):
        attempt = 0
        while attempt < self.max_attempts:
            try:
                transfer = self.get_transfer()
                # 上传时，远程要求是文件夹
                transfer.upload(sql_file_path, "/tmp/")
                # 导入时要求是文件下的数据文件
                remote_sql_path = f"/tmp/{os.path.basename(sql_file_path)}"
                command = f"{self.remote_mysql_path} -u {self.mysqlusername} -p{self.mysqlpassword} -h {self.mysqlhost} --port={self.mysqlport} {database_name} < {remote_sql_path}"
                stin, stout, error = self.execute_ssh_command(command)
                
                # 检查 error 变量
                if error and "error" in error.lower():
                    raise Exception(error)
                
                # 删除临时文件
                delete_command = f"sudo rm {remote_sql_path}"
                self.execute_ssh_command(delete_command)
                logging.info(f"Database '{database_name}' imported successfully from remote SQL file.")
                break
            except Exception as e:
                attempt += 1
                logging.error(f"Error occurred while importing the database from remote: {e}")
                if attempt < self.max_attempts:
                    logging.info(f"Retrying... ({attempt}/{self.max_attempts})")
                    sleep(self.sleep_time)
                else:
                    logging.error("Max retries reached. Failed to import database.")
                    raise

    # 如果指定库，还指定表，那么，必须解析sql_file_path文件提取指定的库和指定表的语句，再执行导入操作
    def import_table(self, database_name, table_name, sql_file_path):
        if self.location_type == 'local':
            self.import_table_local(database_name, table_name, sql_file_path)
        elif self.location_type == 'remote':
            self.import_table_remote(database_name, table_name, sql_file_path)
        else:
            logging.error(f"Invalid location_type: {self.location_type}")

    def extract_table_sql(self, sql_file_path, table_name):
        with open(sql_file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        table_sql = []
        in_table = False

        for line in lines:
            if line.strip().startswith(f"CREATE TABLE `{table_name}`") or line.strip().startswith(f"INSERT INTO `{table_name}`"):
                in_table = True
            if in_table:
                table_sql.append(line)
                if line.strip().endswith(';'):
                    in_table = False

        return ''.join(table_sql)

    def import_table_local(self, database_name, table_name, sql_file_path):
        try:
            # 提取指定表的SQL语句
            table_sql = self.extract_table_sql(sql_file_path, table_name)
            if not table_sql:
                logging.error(f"No SQL statements found for table '{table_name}' in file '{sql_file_path}'")
                return

            # 删除表（如果存在）
            self.delete_table(database_name, table_name)

            # 连接到数据库并执行SQL语句
            with self.engine.connect() as conn:
                conn.execute(text(f"USE {database_name}"))
                for statement in table_sql.split(';'):
                    if statement.strip():
                        conn.execute(text(statement))
            logging.info(f"Table '{table_name}' imported successfully.")
        except Exception as e:
            logging.error(f"Error occurred while importing the table: {e}")
            raise
       
    def import_table_remote(self, database_name, table_name, sql_file_path):
        try:
            #sq_file_path提供的是数据库文件，它既可以在远程，也可以在本地提供
            command = f"if [ -f {sql_file_path} ]; then echo 'true'; else echo 'false'; fi"
            _, stdout, stderr = self.execute_ssh_command(command)
            #数据库文件在远程
            if stdout.strip() == 'true':
                logging.info(f"Importing existing remote SQL file: {sql_file_path} to table: {table_name} in database: {database_name}")
                # 提取指定表的SQL语句
                table_sql = self.extract_table_sql(sql_file_path, table_name)
                if not table_sql:
                    logging.error(f"No SQL statements found for table '{table_name}' in file '{sql_file_path}'")
                    return

                # 删除表（如果存在）
                self.delete_table(database_name, table_name)

                # 执行导入命令
                command = f"{self.remote_mysql_path} -u {self.mysqlusername} -p{self.mysqlpassword} -h {self.mysqlhost} --port={self.mysqlport} {database_name} -e \"{table_sql}\""
                self.execute_ssh_command(command)
                logging.info(f"Table '{table_name}' imported successfully to remote server.")
            else:
                logging.info(f"SQL file: {sql_file_path} does not exist remotely, uploading from local.")
                #提供的数据库文件在本地，所以要上传到远程
                transfer = self.get_transfer()
                transfer.upload(sql_file_path, sql_file_path)

                # 提取指定表的SQL语句
                table_sql = self.extract_table_sql(sql_file_path, table_name)
                if not table_sql:
                    logging.error(f"No SQL statements found for table '{table_name}' in file '{sql_file_path}'")
                    return

                # 删除表（如果存在）
                self.delete_table(database_name, table_name)

                # 执行导入命令
                command = f"{self.remote_mysql_path} -u {self.mysqlusername} -p{self.mysqlpassword} -h {self.mysqlhost} --port={self.mysqlport} {database_name} -e \"{table_sql}\""
                self.execute_ssh_command(command)
                logging.info(f"Table '{table_name}' imported successfully to remote server.")
        except Exception as e:
            logging.error(f"Error occurred while importing the table to remote server: {e}")
            raise
           
    
    def export_database(self, database_name, sql_file_path):
        if self.location_type == 'local':
            self.export_database_local(database_name, sql_file_path)
        elif self.location_type == 'remote':
            self.export_database_remote(database_name, sql_file_path)
        else:
            logging.error(f"Invalid location_type: {self.location_type}")
    # 导入指定库
    def export_database_local(self, database_name, sql_file_path):
        try:
            command = f"{self.local_mysqldump_path} -u {self.mysqlusername} -p{self.mysqlpassword} -h {self.mysqlhost} --port={self.mysqlport} {database_name} > {sql_file_path}"
            subprocess.run(command, shell=True, check=True)
            logging.info(f"Database '{database_name}' exported successfully.")
        except Exception as e:
            logging.error(f"Error occurred while exporting the database: {e}")
            raise
     
    def export_database_remote(self, database_name, sql_file_path):
        try:
            # 在远程服务器上执行导出命令
            remote_sql_file_path = os.path.join('/tmp', os.path.basename(sql_file_path))
            command = f"{self.remote_mysqldump_path} -u {self.mysqlusername} -p{self.mysqlpassword} -h {self.mysqlhost} --port={self.mysqlport} {database_name} > {remote_sql_file_path}"
            self.execute_ssh_command(command)

            # 下载SQL文件到本地
            transfer = self.get_transfer()
            transfer.download(remote_sql_file_path, sql_file_path)
            logging.info(f"Database '{database_name}' exported successfully from remote server.")
        except Exception as e:
            logging.error(f"Error occurred while exporting the database from remote server: {e}")
            raise
         
    # 导入指定表，不同于导入，他可以直接在命令行指定要导入的表
    def export_table(self, database_name, table_name, sql_file_path):
        if self.location_type == 'local':
            self.export_table_local(database_name, table_name, sql_file_path)
        elif self.location_type == 'remote':
            self.export_table_remote(database_name, table_name, sql_file_path)
        else:
            logging.error(f"Invalid location_type: {self.location_type}")

    def export_table_local(self, database_name, table_name, sql_file_path):
        try:
            command = f"{self.local_mysqldump_path} -u {self.mysqlusername} -p{self.password} -h {self.mysqlhost} --port={self.mysqlport} {database_name} {table_name} > {sql_file_path}"
            subprocess.run(command, shell=True, check=True)
            logging.info(f"Table '{table_name}' in database '{database_name}' exported successfully.")
        except Exception as e:
            logging.error(f"Error occurred while exporting the table: {e}")
            raise
 
    def export_table_remote(self, database_name, table_name, sql_file_path):
        try:
            # 在远程服务器上执行导出命令
            remote_sql_file_path = os.path.join('/tmp', os.path.basename(sql_file_path))
            command = f"{self.remote_mysqldump_path} -u {self.mysqlusername} -p{self.mysqlpassword} -h {self.mysqlhost} --port={self.mysqlport} {database_name} {table_name} > {remote_sql_file_path}"
            self.execute_ssh_command(command)

            # 下载SQL文件到本地
            transfer = self.get_transfer()
            transfer.download(remote_sql_file_path, sql_file_path)
            logging.info(f"Table '{table_name}' in database '{database_name}' exported successfully from remote server.")
        except Exception as e:
            logging.error(f"Error occurred while exporting the table from remote server: {e}")
            raise
        
    def get_non_system_databases(self, command_prefix):
        command_show_dbs = (f"{command_prefix} -u{self.mysqlusername} -p{self.mysqlpassword} "
                            f"-h{self.mysqlhost} --port={self.mysqlport} -e \"SHOW DATABASES;\"")
        print(f"Executing command to fetch databases: {command_show_dbs}")

        if self.location_type == 'local':
            result = subprocess.run(command_show_dbs, shell=True, capture_output=True, text=True)
            output = result.stdout
            error = result.stderr
        else:
            _, output, error = self.execute_ssh_command(command_show_dbs)

        if error.strip() and 'Using a password on the command line interface can be insecure' not in error:
            raise Exception(f"Error fetching databases: {error}")

        if not output.strip():
            raise Exception("No output received from SHOW DATABASES command")

        databases = output.strip().split('\n')
        non_system_databases = [db for db in databases if db not in ('Database', 'mysql', 'information_schema', 'performance_schema', 'sys')]
        print(f"Non-system databases fetched: {non_system_databases}")
        return non_system_databases

    def get_non_system_databases_local(self):
        return self.get_non_system_databases(self.local_mysql_path)

    def get_non_system_databases_remote(self):
        return self.get_non_system_databases(self.remote_mysql_path)

    def export_all_databases_to_sql_file(self, sql_file_path):
        try:
            if self.location_type == 'local':
                try:
                    databases = self.get_non_system_databases_local()
                    if not databases:
                        print("No databases found or failed to connect to the MySQL server.")
                        return

                    database_list = ' '.join(databases)
                    command = (f"{self.local_mysqldump_path} -u {self.mysqlusername} "
                               f"-p{self.mysqlpassword} -h {self.mysqlhost} "
                               f"--port={self.mysqlport} --add-drop-database --databases {database_list} > {sql_file_path}")
                    subprocess.run(command, shell=True, check=True)
                    logging.info(f"All databases exported successfully.")
                except Exception as e:
                    logging.error(f"Error occurred while exporting all databases: {e}")
                    raise
            elif self.location_type == 'remote':
                try:
                    databases = self.get_non_system_databases_remote()
                    if not databases:
                        print("No databases found or failed to connect to the MySQL server.")
                        return

                    remote_sql_file_path = os.path.join('/tmp', os.path.basename(sql_file_path))
                    database_list = ' '.join(databases)
                    remote_command = (f"{self.remote_mysqldump_path} -u{self.mysqlusername} "
                                      f"-p{self.mysqlpassword} -h {self.mysqlhost} "
                                      f"--port={self.mysqlport} --add-drop-database --databases {database_list} > {remote_sql_file_path}")
                    print(f"Executing remote command to export databases: {remote_command}")
                    _, output, error = self.execute_ssh_command(remote_command)

                    print(f"Remote command export output: {output}")
                    print(f"Remote command export error output: {error}")

                    if error.strip() and 'Using a password on the command line interface can be insecure' not in error:
                        raise Exception(f"Error exporting databases remotely: {error}")

                    transfer = self.get_transfer()
                    transfer.download(remote_sql_file_path, '/tmp')

                    print(f"All databases exported successfully from remote server.")
                except Exception as e:
                    print(f"Error occurred while exporting the database from remote server: {e}")
                    raise
        except Exception as e:
            logging.error(f"Error occurred while exporting all databases: {e}")
            raise

    def import_all_databases_from_sql_file(self, sql_file_path):
        try:
            if self.location_type == 'local':
                command = f"{self.local_mysql_path} -u {self.mysqlusername} -p{self.mysqlpassword} -h {self.mysqlhost} --port={self.mysqlport} < {sql_file_path}"
                subprocess.run(command, shell=True, check=True)

            elif self.location_type == 'remote':
                attempt = 0
                while attempt < self.max_attempts:
                    try:
                        transfer = self.get_transfer()
                        transfer.upload(sql_file_path, "/tmp/")
                        remote_sql_path = f"/tmp/{os.path.basename(sql_file_path)}"
                        remote_command = f"{self.remote_mysql_path} -u {self.mysqlusername} -p{self.mysqlpassword} -h {self.mysqlhost} --port={self.mysqlport} < {remote_sql_path}"
                        _, _, error = self.execute_ssh_command(remote_command)

                        if error and "error" in error.lower():
                            raise Exception(error)

                        delete_command = f"sudo rm {remote_sql_path}"
                        self.execute_ssh_command(delete_command)
                        logging.info(f"All databases imported successfully from remote SQL file.")
                        break
                    except Exception as e:
                        attempt += 1
                        logging.error(f"Error occurred while importing the database from remote: {e}")
                        if attempt < self.max_attempts:
                            logging.info(f"Retrying... ({attempt}/{self.max_attempts})")
                            sleep(self.sleep_time)
                        else:
                            logging.error("Max retries reached. Failed to import database.")
                            raise
            else:
                logging.error(f"Invalid location_type: {self.location_type}")
                return
        except Exception as e:
            logging.error(f"Error occurred while importing all databases from SQL file: {e}")
            raise
    
    def export_table_to_dataframe(self, database_name, table_name=None, query=None):
        try:
            with self.engine.connect() as connection:
                connection.execute(text(f"USE {database_name}")) 
                if query:
                    logging.warning("Both table name anquery provided. Ignoring the table name and using the provided query.")
                    final_query = query
                elif table_name:
                    final_query = f"SELECT * FROM {table_name}"
                else:
                    logging.error("Either table name or query must be provided.")
                    return None

                df = pd.read_sql(final_query, connection)
                logging.info("Data exported successfully.")
                return df
        except SQLAlchemyError as e:
            logging.error(f"Error occurred while importing data: {e}")
            return None

    def import_dataframe_to_table(self, dataframe, database_name, table_name):
        if not table_name:
            logging.error("必须提供表名。")
            return

        try:
            with self.engine.connect() as connection:
                connection.execute(text(f"USE {database_name}"))

                # 获取DataFrame的列名和数据类型
                dtype_mapping = {
                    'int64': 'INT',
                    'float64': 'FLOAT',
                    'object': 'TEXT',
                    'bool': 'BOOLEAN',
                    'datetime64[ns]': 'DATETIME'
                    # 添加其他数据类型的映射
                }
                columns_definition = ', '.join([f"{column} {dtype_mapping[str(dataframe[column].dtype)]}" for column in dataframe.columns])

                # 创建新表或替换现有表
                connection.execute(text(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_definition})"))

                # 转换DataFrame的数据类型以匹配表的定义
                dataframe = dataframe.astype({column: str(dataframe[column].dtype) for column in dataframe.columns})

                # 导出DataFrame到数据库表
                dataframe.to_sql(table_name, con=self.engine, if_exists='replace', index=False)

                logging.info(f"DataFrame数据成功导入到表'{table_name}'。")
        except SQLAlchemyError as e:
            logging.error(f"导入DataFrame到表时发生错误：{e}")
            raise
        
           
if __name__ == "__main__":
    
    #数据库连接参数
    mysqlusername = "myrt"
    mysqlpassword="123456"
    # mysqlhost = "127.0.0.1"
    mysqlhost = "47.103.135.26"
    mysqlport = 3300
    
    #定义创建远程容器的尝试次数和时间
    max_attempts = 10
    sleep_time = 5 
                    
    # 要连接的数据库容器是否开启
    container_is_up = False
    
    # 以下只有容器不存在的情况下才需要传入参数
    
    #定义要创建的服务（容器）
    service_name = ["p0_s_mysql_master_1"]
    
    #定义要创建的容器是本地还是远程    
    location_type = 'remote'
        
    #定义创建容器的yaml文件及其相关文件的地址
    local_path = '/home/zym/container/'
    remote_path = '/home/zym/'
    # 定义远程连接时所需要的主机地址，用户名和密钥或密码
    remote_host = '47.103.135.26'
    remote_user = 'zym'
    private_key_path = '/home/zym/.ssh/new_key'
    remote_password = "alyfwqok"
    
    # MySQL 和 mysqldump 命令的绝对路径，以便对数据库进行导入和导出操作
    local_mysql_path = '/home/zym/anaconda3/bin/mysql'
    local_mysqldump_path = '/home/zym/anaconda3/bin/mysqldump'
    remote_mysql_path = '/home/zym/anaconda3/bin/mysql'
    remote_mysqldump_path = '/home/zym/anaconda3/bin/mysqldump'
    

    
    db = MySQLDatabase(mysqlusername=mysqlusername, mysqlpassword=mysqlpassword, mysqlhost=mysqlhost, mysqlport=mysqlport,
                       container_is_up=container_is_up,
                       service_name=service_name,
                       location_type=location_type,
                       local_path=local_path,remote_path=remote_path,
                       remote_host=remote_host,remote_user=remote_user,private_key_path=private_key_path,remote_password=remote_password,max_attempts=max_attempts,sleep_time=sleep_time,
                       local_mysql_path=local_mysql_path,local_mysqldump_path=local_mysqldump_path, remote_mysql_path=remote_mysql_path,remote_mysqldump_path=remote_mysqldump_path)  
    try:       
        # db.create_database("example_database", charset='utf8', collation='utf8_unicode_ci')
        # print("Database created successfully.")      
        # fields = ["id INT AUTO_INCREMENT PRIMARY KEY", "name VARCHAR(255)", "age INT"]
        # db.create_table(database_name="example_database", table_name="example_table", fields=fields)
        # print("Table created successfully.")
        # data = [(None, 'Alice', 30), (None, 'Bob', 25)]
        # columns = ['id', 'name', 'age']
        # db.insert_data(database_name="example_database", table_name="example_table", data=data, columns=columns)
        # db.select_data(database_name="example_database", table_name="example_table")
        # db.export_database(database_name="example_database", sql_file_path="example_database.sql")
        # db.import_database(database_name="meiduo_mall", sql_file_path=r"C:\Users\zym\Desktop\web_meiduo_mall_docker\backend\mysql\master_db2.sql")  
        db.import_database('mydb',"/home/zym/web_meiduo_mall_docker/backend/mysql/master_db2.sql")
        # df = db.import_table_to_dataframe(database_name="example_database", table_name="example_table")
        # db.export_dataframe_to_table(dataframe=df, database_name="example_database", table_name="new_table")
        # db.delete_table(database_name="example_database", table_name="example_table")
        # # db.delete_database("example_database")
        # tb_spu=db.select_data('meiduo_mall', 'tb_spu')
        # print(tb_spu)

    except Exception as e:
        logging.error(f"An error occurred: {e}")

