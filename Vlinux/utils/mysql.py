import logging
import os
import re
import pandas as pd
import subprocess
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from file import FileTransfer, SSHSingleton


class MySQLDatabase:
    def __init__(self, mysqlusername, mysqlpassword, mysqlhost='127.0.0.1', mysqlport=3306,
                 location_type='local',
                 remote_host=None, remote_user=None, remote_password=None, private_key_path=None,
                 local_mysql_path='/home/zym/anaconda3/bin/mysql', local_mysqldump_path='/home/zym/anaconda3/bin/mysqldump',
                 remote_mysql_path='/home/zym/anaconda3/bin/mysql', remote_mysqldump_path='/home/zym/anaconda3/bin/mysqldump'):
        
        # 定义数据库连接参数，包括数据库的用户名、数据库的密码、数据库所在主机地址和端口
        # 注意数据库的主机地址和端口，既可以是本地，也可以是远程
        self.mysqlusername = mysqlusername
        self.mysqlpassword = mysqlpassword
        self.mysqlhost = mysqlhost
        self.mysqlport = mysqlport
        
        # 定义是否是远程连接的标志，如果是远程连接，则需要提供远程主机的相关信息
        # 远程连接只有在导入和导出远程的数据库和表到本地文件系统和dataframe时才需要，其他操作都不需要,因为，数据库的其他操作既可以在本地也可以在远程操作，如果数据库在本地，则host为本地IP地址，否则为远程IP地址，
        # 只有在导入导出数据库时，因为要运行命令行，所以才分为本地和远程，不过，远程数据库导出的文件会被下载到本地，而导入远程数据库的文件如果远程没有，则会从本地上传到远程，然后再执行导入
        self.location_type = location_type
        
        # 定义远程主机而不是数据库的的相关信息
        self.remote_host = remote_host
        
        # 定义远程用户信息
        self.remote_user = remote_user
        
        # 定义远程主机的私钥路径,密钥和密码二选一
        self.remote_password = remote_password
        self.private_key_path = private_key_path
        
        # 定义SSH单例对象
        self.ssh_singleton = SSHSingleton()
        # MySQL 和 mysqldump 命令的绝对路径
        self.local_mysql_path = local_mysql_path
        self.local_mysqldump_path = local_mysqldump_path
        self.remote_mysql_path = remote_mysql_path
        self.remote_mysqldump_path = remote_mysqldump_path         
              
        try:
            self.engine = create_engine(f"mysql+pymysql://{mysqlusername}:{mysqlpassword}@{mysqlhost}:{mysqlport}/")
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
        transfer = FileTransfer(remote_host=self.remote_host, remote_user=self.remote_user, remote_password=self.remote_password,
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
        try:
            transfer = self.get_transfer()         
            # 上传时，远程要求时文件夹
            transfer.upload(sql_file_path, "/tmp/")
            # 导入时要求时文件下的数据文件
            remote_sql_path = f"/tmp/{os.path.basename(sql_file_path)}"
            command = f"{self.remote_mysql_path} -u {self.mysqlusername} -p{self.mysqlpassword} -h 127.0.0.1 --port={self.mysqlport} {database_name} < {remote_sql_path}"
            self.execute_ssh_command(command)
            logging.info(f"Database '{database_name}' imported successfully from remote SQL file.")
        except Exception as e:
            logging.error(f"Error occurred while importing the database from remote: {e}")
            raise
    
    def export_database(self, database_name, sql_file_path):
        if self.location_type == 'local':
            self.export_database_local(database_name, sql_file_path)
        elif self.location_type == 'remote':
            self.export_database_remote(database_name, sql_file_path)
        else:
            logging.error(f"Invalid location_type: {self.location_type}")
    
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
            command = f"if [ -f {sql_file_path} ]; then echo 'true'; else echo 'false'; fi"
            _, stdout, stderr = self.execute_ssh_command(command)
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

    def import_all_databases_from_sql_file(self, sql_file_path):
        if self.location_type == 'local':
            self.import_all_databases_from_sql_file_local(sql_file_path)
        elif self.location_type == 'remote':
            self.import_all_databases_from_sql_file_remote(sql_file_path)
        else:
            logging.error(f"Invalid location_type: {self.location_type}")

    def import_all_databases_from_sql_file_local(self, sql_file_path):
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as file:
                sql_content = file.read()

            database_names = re.findall(r'CREATE DATABASE IF NOT EXISTS `([^`]+)`;', sql_content)

            for db_name in database_names:
                db_sql_path = f"{os.path.splitext(sql_file_path)[0]}_{db_name}.sql"
                
                with open(db_sql_path, 'w', encoding='utf-8') as db_file:
                    db_file.write(f"USE `{db_name}`;\n")
                    match = re.search(f'CREATE DATABASE IF NOT EXISTS `{db_name}`;.*?USE `{db_name}`;(.*?)-- Dumping', sql_content, re.DOTALL)
                    if match:
                        db_file.write(match.group(1))

                self.delete_database(db_name)
                self.import_database_local(db_name, db_sql_path)
                os.remove(db_sql_path)

            logging.info("All databases imported successfully.")
        except Exception as e:
            logging.error(f"Error occurred while importing all databases from SQL file: {e}")
            raise

    def import_all_databases_from_sql_file_remote(self, sql_file_path):
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as file:
                sql_content = file.read()

            database_names = re.findall(r'CREATE DATABASE IF NOT EXISTS `([^`]+)`;', sql_content)

            for db_name in database_names:
                db_sql_path = f"{os.path.splitext(sql_file_path)[0]}_{db_name}.sql"
                
                with open(db_sql_path, 'w', encoding='utf-8') as db_file:
                    db_file.write(f"USE `{db_name}`;\n")
                    match = re.search(f'CREATE DATABASE IF NOT EXISTS `{db_name}`;.*?USE `{db_name}`;(.*?)-- Dumping', sql_content, re.DOTALL)
                    if match:
                        db_file.write(match.group(1))

                self.delete_database(db_name)
                self.import_database_remote(db_name, db_sql_path)
                os.remove(db_sql_path)

            logging.info("All databases imported successfully.")
        except Exception as e:
            logging.error(f"Error occurred while importing all databases from SQL file: {e}")
            raise
        
    #把所有的数据库都导出到一个目录下，该目录下的一个文件对应一个数据库
    def export_all_databases_to_sql_directory(self, output_directory):
        try:
            databases = self.get_all_databases()
            if self.location_type == 'local':
                for database in databases:
                    sql_file_path = os.path.join(output_directory, f"{database}.sql")
                    self.export_database_local(database, sql_file_path)
            elif self.location_type == 'remote':
                for database in databases:
                    sql_file_path = os.path.join(output_directory, f"{database}.sql")
                    self.export_database_remote(database, sql_file_path)
            else:
                logging.error(f"Invalid location_type: {self.location_type}")
            logging.info("All databases exported successfully.")
        except Exception as e:
            logging.error(f"Error occurred while exporting all databases: {e}")
            raise
    #把所有的数据库都导出到同一个文件中   
    def export_all_databases_to_sql_file(self, output_file_path):
        try:
            databases = self.get_all_databases()
            
            # 打开输出文件
            with open(output_file_path, 'w') as outfile:
                # 处理本地数据库
                if self.location_type == 'local':
                    for database in databases:
                        outfile.write(f"-- Database: {database}\n\n")
                        command = (f"{self.local_mysqldump_path} -u {self.mysqlusername} "
                                f"-p{self.mysqlpassword} -h {self.mysqlhost} "
                                f"--port={self.mysqlport} {database}")
                        
                        result = subprocess.run(command, shell=True, capture_output=True, text=True)
                        
                        if result.returncode == 0:
                            outfile.write(result.stdout)
                            outfile.write("\n\n")
                        else:
                            logging.error(f"Error occurred while exporting database {database}: {result.stderr}")
                
                # 处理远程数据库
                elif self.location_type == 'remote':
                    for database in databases:
                        outfile.write(f"-- Database: {database}\n\n")
                        remote_sql_file_path = f"/tmp/{database}.sql"
                        command = (f"{self.remote_mysqldump_path} -u {self.mysqlusername} "
                                f"-p{self.mysqlpassword} -h {self.mysqlhost} "
                                f"--port={self.mysqlport} {database} > {remote_sql_file_path}")
                        
                        self.execute_ssh_command(command)
                        
                        transfer = self.get_transfer()
                        local_temp_path = os.path.join('/tmp', f"{database}.sql")
                        transfer.download(remote_sql_file_path, local_temp_path)
                        
                        with open(local_temp_path, 'r') as temp_file:
                            outfile.write(temp_file.read())
                            outfile.write("\n\n")
                        
                        os.remove(local_temp_path)
                else:
                    logging.error(f"Invalid location_type: {self.location_type}")
            
            logging.info("All databases exported successfully to a single file.")
        
        except Exception as e:
            logging.error(f"Error occurred while exporting all databases: {e}")
            raise


    def get_all_databases(self):
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SHOW DATABASES"))
                databases = [row[0] for row in result]
                return databases
        except SQLAlchemyError as e:
            logging.err+r(f"An error occurred while retrieving all databases: {e}")
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
    mysqlusername = "mydb"
    mysqlpassword="123456"
    # mysqlhost = "127.0.0.1"
    mysqlhost = "47.103.135.26"
    mysqlport = 3306
    #远程连接参数
    location_type = "remote"
    
    remote_host = "47.103.135.26"
    remote_user = "root"
    
    #密钥和密码二选一
    # remote_password = "alyfwqok"
    private_key_path = "/home/zym/.ssh/id_rsa"
    
    #mydwL和mysqldump命令的绝对路径    
    local_mysql_path = "/home/zym/anaconda3/bin/mysql"
    local_mysqldump_path = "/home/zym/anaconda3/bin/mysqldump"
    remote_mysql_path = "/home/zym/anaconda3/bin/mysql"
    remote_mysqldump_path = "/home/zym/anaconda3/bin/mysqldump"
    
    db = MySQLDatabase(mysqlusername=mysqlusername, mysqlpassword=mysqlpassword, mysqlhost=mysqlhost, mysqlport=mysqlport,
                       location_type='remote',
                       remote_host=remote_host,remote_user=remote_user,private_key_path=private_key_path,
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
        db.import_database('mydream',"/home/zym/web_meiduo_mall_docker/backend/mysql/master_db2.sql")
        # df = db.import_table_to_dataframe(database_name="example_database", table_name="example_table")
        # db.export_dataframe_to_table(dataframe=df, database_name="example_database", table_name="new_table")
        # db.delete_table(database_name="example_database", table_name="example_table")
        # # db.delete_database("example_database")
        # tb_spu=db.select_data('meiduo_mall', 'tb_spu')
        # print(tb_spu)

    except Exception as e:
        logging.error(f"An error occurred: {e}")

    # finally:
    #     db.close_connection()

