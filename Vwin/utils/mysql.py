import pymysql
import pandas as pd
import logging
import subprocess
import os
from file import FileTransfer, SSHSingleton


class MySQLDatabase:
    def __init__(self, username, password, host='127.0.0.1', port=3306,
                 location_type='local',
                 remote_host=None, remote_user=None, remote_password=None,private_key_path=None,
                 ):
        # 定义数据库连接参数，包括用户名、密码、主机地址和端口
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        
        # 定义是否是远程连接的标志，如果是远程连接，则需要提供远程主机的相关信息
        self.location_type = location_type
        # 定义远程主机的相关信息
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.remote_password = remote_password
        # 定义远程主机的私钥路径,密钥和密码二选一
        self.private_key_path = private_key_path
        # 定义SSH单例对象
        self.ssh_singleton = SSHSingleton()
        

        try:
            self.connection = pymysql.connect(host=self.host, user=self.username, password=self.password,
                                              port=self.port, cursorclass=pymysql.cursors.DictCursor)
            self.cursor = self.connection.cursor()
        except pymysql.Error as e:
            logging.error(f"Error occurred while connecting to the database: {e}")
            raise

    def _establish_ssh_connection(self):
        self.ssh_singleton.connect(self.remote_host, self.remote_user, password=self.remote_password, private_key_path=self.private_key_path)
    
    def get_remote_file_transfer(self):        
            transfer=FileTransfer(remote_host=self.host, remote_user=self.username, remote_password=self.password, private_key_path=self.private_key_path)
            self._establish_ssh_connection()
            ssh=self.ssh_singleton.get_ssh()            
            return transfer,ssh
    
    
    def create_user_and_grant_privileges(self, new_user, new_user_password, pri_database='*', pri_table='*', pri_host='%'):
        try:
            self.cursor.execute(f"CREATE USER IF NOT EXISTS '{new_user}'@'{pri_host}' IDENTIFIED BY '{new_user_password}'")
            self.cursor.execute(f"GRANT ALL PRIVILEGES ON {pri_database}.{pri_table} TO '{new_user}'@'{pri_host}'")
            self.cursor.execute("FLUSH PRIVILEGES")
            logging.info(f"User '{new_user}' created and granted privileges successfully.")
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

    def import_table(self, database_name, tablename,sql_file_path):
        if self.location_type == 'local':
            self.import_table_local(database_name, tablename,sql_file_path)
        elif self.location_type == 'remote':
            self.import_table_remote(database_name, tablename,sql_file_path)
        else:
            logging.error(f"Invalid location_type: {self.location_type}")

    def export_table(self, database_name, tablename,sql_file_path):
        if self.location_type == 'local':
            self.export_table_local(database_name, tablename,sql_file_path)
        elif self.location_type == 'remote':
            self.export_table_remote(database_name, tablename,sql_file_path)
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

    def import_database_remote(self, database_name, sql_file_path):
        try:
            # 在远程服务器上检查文件是否存在
            transfer,ssh=self.get_remote_file_transfer()
            _, stdout, stderr = ssh.exec_command(f"if [ -f {sql_file_path} ]; then echo 'true'; else echo 'false'; fi")
            if stdout.read().strip() == b'true':
                # 如果远程文件存在，直接导入到数据库
                logging.info(f"Importing existing remote SQL file: {sql_file_path} to database: {database_name}")
                ssh.exec_command(f"/home/zym/anaconda3/bin/mysql -h 127.0.0.1 --port-3306 -u {self.username} -p{self.password} {database_name} < {sql_file_path}")
            else:                
                # 获取远程文件传输对象
                transfer,ssh=self.get_remote_file_transfer()
                # 先将本地 SQL 文件上传到远程服务器的/tmp目录下
                remote_file_path = f'/tmp/{os.path.basename(sql_file_path)}'
               
                logging.info(f"Uploading local SQL file: {sql_file_path} to remote: '/tmp/{os.path.basename(sql_file_path)}'")
                transfer.upload(sql_file_path,'/tmp/')
                
                #再导入到数据库
                transfer, ssh = self.get_remote_file_transfer()
                logging.info(f"Importing uploaded SQL file: {remote_file_path} to database: {database_name}")
               
                ssh.exec_command(f"/home/zym/anaconda3/bin/mysql -h 127.0.0.1 --port=3306 -u {self.username} -p{self.password} {database_name} < {remote_file_path}")
                print('kdk')
                # 删除远程服务器上的临时 SQL 文件
                # # 要重新获取ssh连接，否则会报错
                # transfer, ssh = self.get_remote_file_transfer()
                # logging.info(f"Deleting remote temporary SQL file: {remote_file_path}")
                # ssh.exec_command(f"rm {remote_file_path}")

            logging.info(f"Database '{database_name}' imported successfully.")
        except Exception as e:
            logging.error(f"Error occurred while importing the database remotely: {e}")
            raise

      

    def export_database_remote(self, database_name, sql_file_path):
        try:
            transfer, ssh = self.get_remote_file_transfer()
            remote_file_path = f'/tmp/{os.path.basename(sql_file_path)}'
            # 必须使用tcp连接，不能使用socket连接
            # 必须使用msqldump的绝对路径，否则会报错（因为msyqldump不在系统的环境变量中，如果在，直接用mysqldump就可以了）
            command = f"/home/zym/anaconda3/bin/mysqldump -h 127.0.0.1 --port=3306 -u {self.username} -p{self.password} {database_name} > {remote_file_path}"
            _, stdout, stderr = ssh.exec_command(command)
            exit_status = stdout.channel.recv_exit_status()
          
            # 下载导出的 SQL 文件到本地
            local_file_path = os.path.abspath(sql_file_path)  # 本地存储路径
        
            # 检查本地文件是否存在，不存在则创建一个空文件
            if not os.path.exists(local_file_path):
                open(local_file_path, 'w').close()
            # 下载文件
            transfer.download(local_path=os.path.dirname(local_file_path), remote_path=remote_file_path)

            # 删除远程服务器上的 SQL 文件
            # 要重新获取ssh连接，否则会报错
            transfer, ssh = self.get_remote_file_transfer()
           
            print(f"Executing command to delete remote file: rm {remote_file_path}")            
            _, stdout, stderr = ssh.exec_command(f"rm {remote_file_path}")
            exit_status = stdout.channel.recv_exit_status()       
            logging.info(f"Database '{database_name}' exported successfully to {local_file_path}")            
        except Exception as e:
            logging.error(f"Error occurred while exporting the database remotely: {e}")
            raise





    def import_table_remote(self, database_name, tablename, sql_file_path):
        try:
            # 在远程服务器上检查文件是否存在
            transfer,ssh=self.get_remote_file_transfer()
            _, stdout, stderr = ssh.exec_command(f"if [ -f {sql_file_path} ]; then echo 'true'; else echo 'false'; fi")
            if stdout.read().strip() == b'true':
                # 如果远程文件存在，直接导入到数据库
                logging.info(f"Importing existing remote SQL file: {sql_file_path} to database: {database_name}")
                ssh.exec_command(f"/home/zym/anaconda3/bin/mysql -h 127.0.0.1 --port-3306 -u {self.username} -p{self.password} {database_name} < {sql_file_path}")
            else:                
                # 获取远程文件传输对象
                transfer,ssh=self.get_remote_file_transfer()
                # 先将本地 SQL 文件上传到远程服务器的/tmp目录下
                remote_file_path = f'/tmp/{os.path.basename(sql_file_path)}'
               
                logging.info(f"Uploading local SQL file: {sql_file_path} to remote: '/tmp/{os.path.basename(sql_file_path)}'")
                transfer.upload(sql_file_path,'/tmp/')
                
                #再导入到数据库
                transfer, ssh = self.get_remote_file_transfer()
                logging.info(f"Importing uploaded SQL file: {remote_file_path} to database: {database_name}")
               
                ssh.exec_command(f"/home/zym/anaconda3/bin/mysql -h 127.0.0.1 --port=3306 -u {self.username} -p{self.password} {database_name} < {remote_file_path}")
                print('kdk')
                # 删除远程服务器上的临时 SQL 文件
                # # 要重新获取ssh连接，否则会报错
                # transfer, ssh = self.get_remote_file_transfer()
                # logging.info(f"Deleting remote temporary SQL file: {remote_file_path}")
                # ssh.exec_command(f"rm {remote_file_path}")

            logging.info(f"Database '{database_name}' imported successfully.")
        except Exception as e:
            logging.error(f"Error occurred while importing the database remotely: {e}")
            raise

      

    def export_table_remote(self, database_name, tablename,sql_file_path):
        try:
            transfer, ssh = self.get_remote_file_transfer()
            remote_file_path = f'/tmp/{os.path.basename(sql_file_path)}'
            # 必须使用tcp连接，不能使用socket连接
            # 必须使用msqldump的绝对路径，否则会报错（因为msyqldump不在系统的环境变量中，如果在，直接用mysqldump就可以了）
            command = f"/home/zym/anaconda3/bin/mysqldump -h 127.0.0.1 --port=3306 -u {self.username} -p{self.password} {database_name} {tablename} > {remote_file_path}"
            _, stdout, stderr = ssh.exec_command(command)
            exit_status = stdout.channel.recv_exit_status()
          
            # 下载导出的 SQL 文件到本地
            local_file_path = os.path.abspath(sql_file_path)  # 本地存储路径
        
            # 检查本地文件是否存在，不存在则创建一个空文件
            if not os.path.exists(local_file_path):
                open(local_file_path, 'w').close()
            # 下载文件
            transfer.download(local_path=os.path.dirname(local_file_path), remote_path=remote_file_path)

            # 删除远程服务器上的 SQL 文件
            # 要重新获取ssh连接，否则会报错
            transfer, ssh = self.get_remote_file_transfer()
           
            print(f"Executing command to delete remote file: rm {remote_file_path}")            
            _, stdout, stderr = ssh.exec_command(f"rm {remote_file_path}")
            exit_status = stdout.channel.recv_exit_status()       
            logging.info(f"Database '{database_name}' exported successfully to {local_file_path}")            
        except Exception as e:
            logging.error(f"Error occurred while exporting the database remotely: {e}")
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
    #数据库连接参数
    username = "zym"
    password="123456"
    host = "47.100.19.119"
    port = 3306
    #远程连接参数
    remote_host = "47.100.19.119"
    remote_user = "zym"
    
    #密钥和密码二选一
    # remote_password = "alyfwqok"
    private_key_path = "c:\\Users\\zym\\.ssh\\id_rsa"
    
    db = MySQLDatabase(username="zym", password="123456", host="47.103.135.26", 
                       port=3306,
                       location_type='remote',remote_host=remote_host,remote_user=remote_user,private_key_path=private_key_path)  
    try:       
        db.create_database("example_database", charset='utf8', collation='utf8_unicode_ci')
        print("Database created successfully.")      
        fields = ["id INT AUTO_INCREMENT PRIMARY KEY", "name VARCHAR(255)", "age INT"]
        db.create_table(database_name="example_database", table_name="example_table", fields=fields)
        print("Table created successfully.")
        data = [(None, 'Alice', 30), (None, 'Bob', 25)]
        columns = ['id', 'name', 'age']
        db.insert_data(database_name="example_database", table_name="example_table", data=data, columns=columns)
        db.select_data(database_name="example_database", table_name="example_table")
        # db.export_database(database_name="example_database", sql_file_path="example_database.sql")
        db.import_database(database_name="meiduo_mall", sql_file_path=r"C:\Users\zym\Desktop\web_meiduo_mall_docker\backend\mysql\master_db2.sql")  
        # df = db.import_table_to_dataframe(database_name="example_database", table_name="example_table")
        # db.export_dataframe_to_table(dataframe=df, database_name="example_database", table_name="new_table")
        # db.delete_table(database_name="example_database", table_name="example_table")
        # db.delete_database("example_database")

    except Exception as e:
        logging.error(f"An error occurred: {e}")

    finally:
        db.close_connection()
