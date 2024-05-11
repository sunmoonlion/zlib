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

from container import Container


class MySQLReplication:
    def __init__(self,user_master, password_master, user_slave, password_slave, local_path, remote_path, service_name=None, remote_host=None,
                 
                 remote_user=None, remote_password=None,location_type='local', max_attempts=10, sleep_time=5, private_key_path=None,
                 container_master_exist=True, container_slave_exist=True,
                 db_master_host='127.0.0.1',db_master_port=3306, db_slave_host='127.0.0.1',db_slave_port=3306,
                 repl_user=None,repl_password=None):
        # 定义主从数据库的连接参数，包括主从数据库的用户名和密码
        self.user_master = user_master
        self.password_master = password_master
        self.user_slave = user_slave
        self.password_slave = password_slave
        # 定义容器的初始化参数，在没有提供数据库容器的情况下，将会自动创建容器，有本地和远程两种场景
        self.local_path = local_path
        self.remote_path = remote_path
        self.service_name = service_name
        self.remote_host = remote_host
        self.remote_user = remote_user
        # 定义远程主机的用户名和密码，用于连接到远程主机
        self.remote_password = remote_password
        self.location_type = location_type
        self.max_attempts = max_attempts
        self.sleep_time = sleep_time
        self.private_key_path = private_key_path
        # 定义主从数据库的连接参数，包括主从数据库的主机地址和端口
        self.db_master_host = db_master_host
        self.db_master_port = db_master_port
        self.db_slave_host = db_slave_host
        self.db_slave_port = db_slave_port
        # 定义容器是否存在的标志，如果容器不存在，则会自动创建容器
        self.container_master_exist = container_master_exist
        self.container_slave_exist = container_slave_exist
        # 定义主从数据库的复制用户和密码
        self.repl_user = repl_user
        self.repl_password = repl_password        
       
    def get_database(self, user, password, host, port,local_path, remote_path,service_name, remote_host,remote_user, remote_password,
                                max_attempts, sleep_time, private_key_path,location_type):
        # 定义获取数据库的方法，如果容器不存在，则会自动创建容器
        if self.container_exist:
            container = Container(self.local_path, self.remote_path, self.service_name, self.remote_host, self.remote_user, self.remote_password,
                                self.max_attempts, self.sleep_time, self.private_key_path,self.location_type)
            try:
                container.up_all_services()
                print(f"Container started successfully for {user}.")
            except Exception as e:
                print(f"Failed to start container for {user}: {e}")
        return MySQLDatabase(user, password, host, port, self.private_key_path, self.local_type)

    def get_db_master(self):
        return self.get_database(self.user_master, self.password_master, self.db_master_host, self.db_master_port)

    def get_db_slave(self):
        return self.get_database(self.user_slave, self.password_slave, self.db_slave_host, self.db_slave_port)

        
    # 创建复制用户        
    def create_repl_user(self):
        query = (
            f"CREATE USER IF NOT EXISTS '{self.repl_user}'@'%' "
            f"IDENTIFIED BY '{self.repl_password}';"
            f"GRANT REPLICATION SLAVE ON *.* TO '{self.repl_user}'@'%';"
            "FLUSH PRIVILEGES;"
        )
        db_master=self.get_db_master()
        db_master.cursor.execute(query)
        print("Replication user created successfully.")

    def get_master_status(self):
        query = "SHOW MASTER STATUS;"
        db_master = self.get_db_master()
        db_master.cursor.execute(query)
        result = db_master.cursor.fetchone()  # 使用 fetchone() 获取单个结果行
        if result:
            binlog_file = result['File']
            binlog_position = result['Position']
            print("Master status retrieved successfully.")
            return binlog_file, binlog_position
        return None, None


    def import_data_to_db_slave(self):
        if self.container_exist:            
            # 从主数据库导出数据
            db_master = self.get_db_master()
            if db_master.export_table_to_file(self.database_name, self.table_name, '/data/dump.sql'):
                print("Data exported from master successfully.")
            else:
                raise Exception("Failed to export data from master.")

            # 从主数据库导出的数据导入到从数据库
            try:
                with open('/data/dump.sql', 'r') as sql_file:
                    sql_statements = sql_file.read()

                db_slave = self.get_db_slave()  # 连接到从数据库
                db_slave.cursor.executescript(sql_statements)
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
        db_slave.cursor.execute(query)
        print("Replication configured successfully.")

    def start_replication(self):
        query = "START SLAVE;"
        db_slave=self.get_db_slave()
        db_slave.cursor.execute(query)
        print("Replication started successfully.")

    def check_db_slave_status(self):
        max_attempts = 10  # 设置最大尝试次数
        attempts = 0  # 当前尝试次数
        wait_time = self.sleep_time  # 第一次等待时间

        while attempts < max_attempts:
            time.sleep(wait_time)
            query = "SHOW SLAVE STATUS;"
            db_slave = self.get_db_slave()
            db_slave.cursor.execute(query)
            result = db_slave.cursor.fetchone()
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
                wait_time *= 2  # 每次尝试等待时间加倍，可以根据实际情况调整


                
                
    def main(self):
        db_master=self.get_db_master()
        db_master.create_user_and_grant_privileges(self.remote_user, self.remote_password)    
        binlog_file, binlog_position = self.get_master_status()
        self.import_data_to_db_slave()
        self.configure_replication(binlog_file, binlog_position)
        self.start_replication()
        self.check_db_slave_status()

if __name__ == "__main__":
    user_master = "root"
    password_master="123456"
    user_slave = "root"
    password_slave="123456"
    local_path = "/home/zym/Desktop/web_meiduo_mall_docker"
    remote_path = "/home/zym/container"
    service_name = "db_master"
    remote_host = "47.103.135.26"
    container_master_exist = False
    container_slave_exist = False
    db_master_host = '47.103.135.26'
    db_master_port = 3306
    db_slave_host = '47.103.135.26'
    db_slave_port = 3306
    repl_user = "repl_user"
    repl_password = "123456"    
    replication = MySQLReplication(user_master=user_master, password_master=password_master, user_slave=user_slave, password_slave=password_slave,
                                   local_path=local_path, remote_path=remote_path, service_name=service_name, remote_host=remote_host, 
                                   container_master_exist=container_master_exist, container_slave_exist=container_slave_exist,
                                   db_master_host=db_master_host, db_master_port=db_master_port, db_slave_host=db_slave_host,
                                   db_slave_port=db_slave_port, repl_user=repl_user, repl_password=repl_password)
    replication.main()