import time
import yaml
import pymysql
import os
import sys

# 获取当前脚本所在的目录
script_dir = os.path.dirname(os.path.abspath(__file__))
# 获取上级目录
parent_dir = os.path.dirname(script_dir)
# 添加上级目录到系统路径中
sys.path.append(parent_dir)

from mysql import MySQLDatabase
from container.container import Container

class MySQLReplication:
    def __init__(self, user_master=None, password_master=None, user_slave=None, password_slave=None, 
                 db_master_host='127.0.0.1', db_master_port=3306, db_slave_host='127.0.0.1', repl_user=None, 
                 repl_password=None, db_slave_port=3307,container_is_up_master=None,container_is_up_slave=None,
                 location_type='local',local_path=None,remote_path=None, service_name_master=None, service_name_slave=None, remote_host=None,
                 remote_user=None, remote_password=None, private_key_path=None, max_attempts=10, sleep_time=5,
                 local_mysql_path=None, local_mysqldump_path=None,remote_mysql_path=None, remote_mysqldump_path=None):
        
        # 定义主从数据库的连接参数，包括主从数据库的用户名和密码
        self.user_master = user_master
        self.password_master = password_master
        self.user_slave = user_slave
        self.password_slave = password_slave
        # 定义主从数据库的连接参数，包括主从数据库的主机地址和端口
        self.db_master_host = db_master_host
        self.db_master_port = db_master_port
        self.db_slave_host = db_slave_host
        self.db_slave_port = db_slave_port
        
        # 定义主从数据库的复制用户和密码
        self.repl_user = repl_user
        self.repl_password = repl_password        
        
        # 定义容器是否存在的标志，如果容器不存在，则会自动创建容器
        self.container_is_up_master = container_is_up_master
        self.container_is_up_slave = container_is_up_slave
        
        # 以下只有容器不存在的情况下才需要传入参数
        #定义要创建的容器是本地还是远程
        self.location_type = location_type        
        # 定义创建容器的初始化参数
        #定义创建容器的yaml文件及其相关文件的地址
        self.local_path = local_path
        self.remote_path = remote_path
        #定义如果远程创建容器，则需提供远程主机地址，用户名和密钥或密码
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.private_key_path = private_key_path
        self.remote_password = remote_password
        # 定义主从服务名称，以分别创建主从容器
        self.service_name_master = service_name_master
        self.service_name_slave = service_name_slave
        #定义创建远程容器的尝试次数和时间
        self.max_attempts = max_attempts
        self.sleep_time = sleep_time
        # MySQL 和 mysqldump 命令的绝对路径，以便对数据库进行导入和导出操作
        self.local_mysql_path = local_mysql_path
        self.local_mysqldump_path = local_mysqldump_path
        self.remote_mysql_path = remote_mysql_path
        self.remote_mysqldump_path = remote_mysqldump_path    

    # def get_database(self, mysqlusername, mysqlpassword, mysqlhost, mysqlport, service_name, container_is_up):
    #     if not container_is_up:
    #         container = Container(self.local_path, self.remote_path, service_name, self.remote_host, self.remote_user, self.remote_password,
    #                               self.location_type, self.max_attempts, self.sleep_time, self.private_key_path)
    #         try:
    #             container.up_services()
    #             print(f"Container started successfully for {mysqlusername}.")
    #         except Exception as e:
    #             print(f"Failed to start container for {mysqlusername}: {e}")
    #     return MySQLDatabase(mysqlusername, mysqlpassword, mysqlhost, mysqlport, self.location_type, self.remote_host, self.remote_user, self.private_key_path,self.remote_password,
    #                          self.local_mysql_path, self.local_mysqldump_path, self.remote_mysql_path, self.remote_mysqldump_path)

    def get_db_master(self):
        
        return MySQLDatabase(self.user_master, self.password_master,  self.db_master_host,self.db_master_port, self.container_is_up_master,
                             self.location_type, self.service_name_master,self.remote_host, self.remote_user, self.private_key_path,self.remote_password,self.max_attempts,self.sleep_time,
                             self.local_mysql_path, self.local_mysqldump_path, self.remote_mysql_path, self.remote_mysqldump_path
                             )

    def get_db_slave(self):
        return MySQLDatabase(self.user_slave, self.password_slave,  self.db_slave_host,self.db_slave_port, self.container_is_up_slave,
                             self.location_type, self.service_name_slave,self.remote_host, self.remote_user, self.private_key_path,self.remote_password,self.max_attempts,self.sleep_time,
                             self.local_mysql_path, self.local_mysqldump_path, self.remote_mysql_path, self.remote_mysqldump_path
                             )

    def create_repl_user(self):
        query = (
            f"CREATE USER IF NOT EXISTS '{self.repl_user}'@'%' "
            f"IDENTIFIED BY '{self.repl_password}';"
            f"GRANT REPLICATION SLAVE ON *.* TO '{self.repl_user}'@'%';"
            "FLUSH PRIVILEGES;"
        )
        db_master = self.get_db_master()
        try:
            with db_master.cursor() as cursor:
                cursor.execute(query)
            db_master.commit()
            print("Replication user created successfully.")
        except Exception as e:
            print(f"Failed to create replication user: {e}")
            db_master.rollback()

    def get_master_status(self):
        query = "SHOW MASTER STATUS;"
        db_master = self.get_db_master()
        try:
            with db_master.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
            if result:
                binlog_file = result['File']
                binlog_position = result['Position']
                print("Master status retrieved successfully.")
                return binlog_file, binlog_position
        except Exception as e:
            print(f"Failed to get master status: {e}")
        return None, None

    def import_data_to_db_slave(self):
        db_master = self.get_db_master()
        try:
            if db_master.export_table_to_file(self.database_name, self.table_name, '/data/dump.sql'):
                print("Data exported from master successfully.")
            else:
                raise Exception("Failed to export data from master.")

            with open('/data/dump.sql', 'r') as sql_file:
                sql_statements = sql_file.read()

            db_slave = self.get_db_slave()
            with db_slave.cursor() as cursor:
                cursor.executescript(sql_statements)
            db_slave.commit()
            print("Data imported to replica successfully.")
        except Exception as e:
            print(f"Failed to import data to replica: {e}")
            raise

    def configure_replication(self, binlog_file, binlog_position):
        query = (
            f"CHANGE MASTER TO MASTER_HOST='{self.db_master_host}',"
            f"MASTER_USER='{self.repl_user}',"
            f"MASTER_PASSWORD='{self.repl_password}',"
            f"MASTER_LOG_FILE='{binlog_file}',"
            f"MASTER_LOG_POS={binlog_position};"
        )
        db_slave = self.get_db_slave()
        try:
            with db_slave.cursor() as cursor:
                cursor.execute(query)
            db_slave.commit()
            print("Replication configured successfully.")
        except Exception as e:
            print(f"Failed to configure replication: {e}")
            db_slave.rollback()

    def start_replication(self):
        query = "START SLAVE;"
        db_slave = self.get_db_slave()
        try:
            with db_slave.cursor() as cursor:
                cursor.execute(query)
            db_slave.commit()
            print("Replication started successfully.")
        except Exception as e:
            print(f"Failed to start replication: {e}")
            db_slave.rollback()

    def check_db_slave_status(self):
        max_attempts = 10
        attempts = 0
        wait_time = self.sleep_time

        while attempts < max_attempts:
            time.sleep(wait_time)
            query = "SHOW SLAVE STATUS;"
            db_slave = self.get_db_slave()
            try:
                with db_slave.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchone()
                if result and result['Slave_IO_Running'] == 'Yes' and result['Slave_SQL_Running'] == 'Yes':
                    print("Replication is running successfully.")
                    break
                else:
                    print("Replication is not yet running. Waiting...")
                    attempts += 1
                    if attempts == max_attempts:
                        print("Maximum attempts reached. Exiting...")
                        break
                    time.sleep(self.sleep_time)
                    wait_time *= 2
            except Exception as e:
                print(f"Failed to check slave status: {e}")

    def main(self):
        try:
            self.create_repl_user()
            binlog_file, binlog_position = self.get_master_status()
            if binlog_file and binlog_position:
                self.import_data_to_db_slave()
                self.configure_replication(binlog_file, binlog_position)
                self.start_replication()
                self.check_db_slave_status()
            else:
                print("Failed to retrieve binlog file and position. Exiting...")
        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    # 定义主从数据库的连接参数，包括主从数据库的用户名和密码
        user_master = 'mydb'
        password_master = '123456'
        user_slave = 'mydb'
        password_slave = '123456'
        # 定义主从数据库的连接参数，包括主从数据库的主机地址和端口
        db_master_host = '47.103.135.26'
        db_master_port = 3306
        db_slave_host = '47.103.135.26'
        db_slave_port = 3307
        
        # 定义主从数据库的复制用户和密码
        repl_user = 'repl_user'
        repl_password = '123456'        
        
        # 定义容器是否存在的标志，如果容器不存在，则会自动创建容器
        container_master_exist = True
        container_slave_exist = True
        
        # 以下只有容器不存在的情况下才需要传入参数
        #定义要创建的容器是本地还是远程
        location_type = 'remote'        
        # 定义创建容器的初始化参数
        #定义创建容器的yaml文件及其相关文件的地址
        local_path = '/home/zym/container'
        remote_path = '/home/zym/container'
        #定义如果远程创建容器，则需提供远程主机地址，用户名和密钥或密码
        remote_host = '47.103.135.26'
        remote_user = 'zym'
        private_key_path = '/home/zym/.sssh/new_key'
        remote_password = "alyfwqok"
        # 定义主从服务名称，以分别创建主从容器
        service_name_master = 'p0_s_mysql_master_1'
        service_name_slave = 'p0_s_mysql_slave_1'
        #定义创建远程容器的尝试次数和时间
        max_attempts = 10
        sleep_time = 5
        # MySQL 和 mysqldump 命令的绝对路径，以便对数据库进行导入和导出操作
        local_mysql_path = '/home/zym/anaconda3/bin/mysql'
        local_mysqldump_path = '/home/zym/anaconda3/bin/mysqldump'
        remote_mysql_path = '/home/zym/anaconda3/bin/mysql'
        remote_mysqldump_path = '/home/zym/anaconda3/bin/mysqldump'

        replication = MySQLReplication(user_master=user_master, password_master=password_master, user_slave=user_slave, password_slave=password_slave,
                                   local_path=local_path, remote_path=remote_path, service_name_master=service_name_master, service_name_slave=service_name_slave, 
                                   remote_host=remote_host,remote_user=remote_user,remote_password=remote_password,private_key_path=private_key_path,
                                   container_master_exist=container_master_exist, container_slave_exist=container_slave_exist,
                                   db_master_host=db_master_host, db_master_port=db_master_port, db_slave_host=db_slave_host,db_slave_port=db_slave_port,
                                   repl_user=repl_user, repl_password=repl_password)
        replication.main()
