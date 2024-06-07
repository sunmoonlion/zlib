import yaml
import pymysql
import os
import sys
from time import sleep

# 获取当前脚本所在的目录
script_dir = os.path.dirname(os.path.abspath(__file__))
# 获取上级目录
parent_dir = os.path.dirname(script_dir)
# 添加上级目录到系统路径中
sys.path.append(parent_dir)

from mysql import MySQLDatabase
from container.container import Container

class MySQLReplication:
    def __init__(self, repl_user=None, repl_password=None, master_datacopying_is_required=False,slave_datacopying_is_required=False,
                 mysqlusername_master=None, mysqlpassword_master=None, mysqlhost_master='127.0.0.1', mysqlport_master=3306,
                 max_attempts_master=10, sleep_time_master=5,
                 container_is_up_master=None,
                 location_type_master='local',
                 service_name_master=None,
                 local_path_master=None, remote_path_master=None, remote_user_master=None, remote_host_master=None, private_key_path_master=None, remote_password_master=None,
                 local_mysql_path_master=None, local_mysqldump_path_master=None, remote_mysql_path_master=None, remote_mysqldump_path_master=None,
                 mysqlusername_slave=None, mysqlpassword_slave=None, mysqlhost_slave='127.0.0.1', mysqlport_slave=3306,
                 max_attempts_slave=10, sleep_time_slave=5,
                 container_is_up_slave=None,
                 location_type_slave='local',
                 service_name_slave=None,
                 local_path_slave=None, remote_path_slave=None, remote_user_slave=None, remote_host_slave=None, private_key_path_slave=None, remote_password_slave=None,
                 local_mysql_path_slave=None, local_mysqldump_path_slave=None, remote_mysql_path_slave=None, remote_mysqldump_path_slave=None):

        # 定义复制的独有参数
        # 定义主从数据库的复制用户和密码
        self.repl_user = repl_user
        self.repl_password = repl_password
        self.master_datacopying_is_required = master_datacopying_is_required
        self.slave_datacopying_is_required = slave_datacopying_is_required

        # 定义主从数据库的连接参数，包括主数据库的主机地址，端口号、用户名和密码

        self.mysqlhost_master = mysqlhost_master
        self.mysqlport_master = mysqlport_master
        self.mysqlusername_master = mysqlusername_master
        self.mysqlpassword_master = mysqlpassword_master

        # 定义试错次数和时间，管理主数据库需要，创建容器也需要
        self.max_attempts_master = max_attempts_master
        self.sleep_time_master = sleep_time_master

        # 定义容器是否存在的标志，如果容器不存在，则会自动创建容器
        self.container_is_up_master = container_is_up_master

        # 以下只有容器不存在的情况下才需要传入参数

        # 定义创建容器的初始化参数

        # 定义要创建的容器是本地还是远程
        self.location_type_master = location_type_master

        # 定义服务名称，以创建容器
        self.service_name_master = service_name_master

        # 定义创建容器的yaml文件及其相关文件的地址
        self.local_path_master = local_path_master
        self.remote_path_master = remote_path_master
        # 定义如果远程创建容器，则需提供远程主机地址，用户名和密钥或密码
        self.remote_host_master = remote_host_master
        self.remote_user_master = remote_user_master
        self.private_key_path_master = private_key_path_master
        self.remote_password_master = remote_password_master

        # MySQL 和 mysqldump 命令的绝对路径，以便对数据库进行导入和导出操作
        self.local_mysql_path_master = local_mysql_path_master
        self.local_mysqldump_path_master = local_mysqldump_path_master
        self.remote_mysql_path_master = remote_mysql_path_master
        self.remote_mysqldump_path_master = remote_mysqldump_path_master

        # 定义从数据库的连接参数，包括主数据库的主机地址，端口号、用户名和密码

        self.mysqlhost_slave = mysqlhost_slave
        self.mysqlport_slave = mysqlport_slave
        self.mysqlusername_slave = mysqlusername_slave
        self.mysqlpassword_slave = mysqlpassword_slave

        # 定义试错次数和时间，管理从数据库需要，创建容器也需要
        self.max_attempts_slave = max_attempts_slave
        self.sleep_time_slave = sleep_time_slave

        # 定义容器是否存在的标志，如果容器不存在，则会自动创建容器
        self.container_is_up_slave = container_is_up_slave

        # 以下只有容器不存在的情况下才需要传入参数

        # 定义创建容器的初始化参数

        # 定义要创建的容器是本地还是远程
        self.location_type_slave = location_type_slave

        # 定义服务名称，以创建容器
        self.service_name_slave = service_name_slave

        # 定义创建容器的yaml文件及其相关文件的地址
        self.local_path_slave = local_path_slave
        self.remote_path_slave = remote_path_slave
        # 定义如果远程创建容器，则需提供远程主机地址，用户名和密钥或密码
        self.remote_user_slave = remote_user_slave
        self.remote_host_slave = remote_host_slave
        self.private_key_path_slave = private_key_path_slave
        self.remote_password_slave = remote_password_slave

        # MySQL 和 mysqldump 命令的绝对路径，以便对数据库进行导入和导出操作
        self.local_mysql_path_slave = local_mysql_path_slave
        self.local_mysqldump_path_slave = local_mysqldump_path_slave
        self.remote_mysql_path_slave = remote_mysql_path_slave
        self.remote_mysqldump_path_slave = remote_mysqldump_path_slave

        self.db_master = self.get_db_master()
        self.db_slave = self.get_db_slave()

    def get_db_master(self):
        db = MySQLDatabase(mysqlusername=self.mysqlusername_master, mysqlpassword=self.mysqlpassword_master, mysqlhost=self.mysqlhost_master, mysqlport=self.mysqlport_master,
                             container_is_up=self.container_is_up_master,
                             service_name=self.service_name_master,
                             location_type=self.location_type_master,
                             local_path=self.local_path_master, remote_path=self.remote_path_master,
                             remote_host=self.remote_host_master, remote_user=self.remote_user_master, private_key_path=self.private_key_path_master,
                             remote_password=self.remote_password_master, max_attempts=self.max_attempts_master, sleep_time=self.sleep_time_master,
                             local_mysql_path=self.local_mysql_path_master, local_mysqldump_path=self.local_mysqldump_path_master,
                             remote_mysql_path=self.remote_mysql_path_master, remote_mysqldump_path=self.remote_mysqldump_path_master)

    # 连接测试
        try:
            with db.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            print("Successfully connected to the master database.")
        except Exception as e:
            print(f"Failed to connect to the master database: {e}")
            raise
        
        return db
    
    def get_db_slave(self):
        db = MySQLDatabase(mysqlusername=self.mysqlusername_slave, mysqlpassword=self.mysqlpassword_slave, mysqlhost=self.mysqlhost_slave, mysqlport=self.mysqlport_slave,
                             container_is_up=self.container_is_up_slave,
                             service_name=self.service_name_slave,
                             location_type=self.location_type_slave,
                             local_path=self.local_path_slave, remote_path=self.remote_path_slave,
                             remote_host=self.remote_host_slave, remote_user=self.remote_user_slave, private_key_path=self.private_key_path_slave,
                             remote_password=self.remote_password_slave, max_attempts=self.max_attempts_slave, sleep_time=self.sleep_time_slave,
                             local_mysql_path=self.local_mysql_path_slave, local_mysqldump_path=self.local_mysqldump_path_slave,
                             remote_mysql_path=self.remote_mysql_path_slave, remote_mysqldump_path=self.remote_mysqldump_path_slave)

     # 连接测试
        try:
            with db.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            print("Successfully connected to the slave database.")
        except Exception as e:
            print(f"Failed to connect to the slave database: {e}")
            raise

        return db
    
    def get_master_status(self):
        query = "SHOW MASTER STATUS;"

        try:
            # 使用 pymysql 直接执行查询
            with self.db_master.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
            if result:
                binlog_file = result[0]  # 根据实际返回的字段顺序来提取
                binlog_position = result[1]  # 根据实际返回的字段顺序来提取
                print("Master status retrieved successfully.")
                return binlog_file, binlog_position
        except Exception as e:
            print(f"Failed to get master status: {e}")
        finally:
            self.db_master.connection.close()
        return None, None

    def import_data_from_db_master_to_db_slave(self):

        try:
            self.db_master.export_all_databases_to_sql_file('/tmp/dump_replication.sql')
            print("Data exported from master successfully.")
        except Exception as e:
            print(f"Failed to export data from master: {e}")
            raise

        try:
            self.db_slave.import_all_databases_from_sql_file('/tmp/dump_replication.sql')
            print("Data imported to slave successfully.")
        except Exception as e:
            print(f"Failed to import data to replica: {e}")
            raise

    def set_up_replication(self):
        binlog_file, binlog_position = self.get_master_status()

        if binlog_file is None or binlog_position is None:
            raise Exception("Failed to get master status.")

        replication_query = f"""
            CHANGE MASTER TO
            MASTER_HOST='{self.mysqlhost_master}',
            MASTER_PORT={self.mysqlport_master},
            MASTER_USER='{self.repl_user}',
            MASTER_PASSWORD='{self.repl_password}',
            MASTER_LOG_FILE='{binlog_file}',
            MASTER_LOG_POS={binlog_position};
        """

        try:
            with self.db_slave.connection.cursor() as cursor:
                cursor.execute("STOP SLAVE IO_THREAD;")  # 停止I/O线程
                cursor.execute(replication_query)
                cursor.execute("START SLAVE;")
            self.db_slave.connection.commit()
            print("Replication set up successfully.")
        except Exception as e:
            print(f"Failed to set up replication: {e}")
            raise
        finally:
            self.db_slave.connection.close()

    def check_replication_status(self):
        query = "SHOW SLAVE STATUS;"
        try:
            with self.db_slave.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                if result:
                    print("Replication status retrieved successfully.")
                    print("Result:", result)
                    io_running = result["Slave_IO_Running"]
                    sql_running = result["Slave_SQL_Running"]
                    if io_running == "Yes" and sql_running == "Yes":
                        print("Replication status: Running")
                    else:
                        print("Replication status: Not running")
                else:
                    print("No replication status found.")
        except pymysql.MySQLError as e:
            print(f"MySQL error occurred: {e.args[0]}, {e.args[1]}")
        except Exception as e:
            print(f"An unexpected error occurred while checking replication status: {e}")

                

if __name__ == "__main__":

    # 定义主从数据库的复制用户和密码
    repl_user = 'repl_user'
    repl_password = '123456'
    
    #是否往主机复制数据 
    master_datacopying_is_required = True
    #是否从主机往从机复制数据
    slave_datacopying_is_required = True
    
    #主机参数  
    
    #数据库连接参数
    mysqlusername_master = "myrt"
    mysqlpassword_master="123456"
    # mysqlhost = "127.0.0.1"
    mysqlhost_master = "47.103.135.26"
    mysqlport_master = 3300
    
    #定义创建远程容器的尝试次数和时间
    max_attempts_master = 10
    sleep_time_master = 5  
    
    # 要连接的数据库容器是否开启
    container_is_up_master = False
    
    # 以下只有容器不存在的情况下才需要传入参数
    
    #定义要创建的服务（容器）
    service_name_master = ["p0_s_mysql_master_1"]
    
    #定义要创建的容器是本地还是远程    
    location_type_master = 'remote'
        
    #定义创建容器的yaml文件及其相关文件的地址
    local_path_master = '/home/zym/container/'
    remote_path_master = '/home/zym/'
    # 定义远程连接时所需要的主机地址，用户名和密钥或密码
    remote_host_master = '47.103.135.26'
    remote_user_master = 'zym'
    private_key_path_master = '/home/zym/.ssh/new_key'
    remote_password_master = "alyfwqok"
    
    # MySQL 和 mysqldump 命令的绝对路径，以便对数据库进行导入和导出操作
    local_mysql_path_master = '/home/zym/anaconda3/bin/mysql'
    local_mysqldump_path_master = '/home/zym/anaconda3/bin/mysqldump'
    remote_mysql_path_master = '/home/zym/anaconda3/bin/mysql'
    remote_mysqldump_path_master = '/home/zym/anaconda3/bin/mysqldump'
    
    #从机参数                
    
    #数据库连接参数
    mysqlusername_slave = "myrt"
    mysqlpassword_slave="123456"
    mysqlhost_slave = "127.0.0.1"
    mysqlport_slave = 4300
    
    #定义创建远程容器的尝试次数和时间
    max_attempts_slave= 10
    sleep_time_slave = 5  
    # 要连接的数据库容器是否开启
    container_is_up_slave = False
    
    # 以下只有容器不存在的情况下才需要传入参数
    
    #定义要创建的服务（容器）
    service_name_slave = ["p0_s_mysql_slave_1"]
    
    #定义要创建的容器是本地还是远程    
    location_type_slave = 'local'
        
    #定义创建容器的yaml文件及其相关文件的地址
    local_path_slave = '/home/zym/container/'
    remote_path_slave = '/home/zym/'
    # 定义远程连接时所需要的主机地址，用户名和密钥或密码
    remote_host_slave = '47.100.19.119'
    remote_user_slave = 'zym'
    private_key_path_slave = '/home/zym/.ssh/new_key'
    remote_password_slave = "alyfwqok"
    
    # MySQL 和 mysqldump 命令的绝对路径，以便对数据库进行导入和导出操作
    local_mysql_path_slave = '/home/zym/anaconda3/bin/mysql'
    local_mysqldump_path_slave = '/home/zym/anaconda3/bin/mysqldump'
    remote_mysql_path_slave = '/home/zym/anaconda3/bin/mysql'
    remote_mysqldump_path_slave = '/home/zym/anaconda3/bin/mysqldump'
    
    

    replication = MySQLReplication(repl_user=repl_user, repl_password=repl_password, master_datacopying_is_required=master_datacopying_is_required, slave_datacopying_is_required= slave_datacopying_is_required,
                mysqlusername_master=mysqlusername_master, mysqlpassword_master=mysqlpassword_master, mysqlhost_master=mysqlhost_master, mysqlport_master=mysqlport_master, 
                max_attempts_master=max_attempts_master, sleep_time_master=sleep_time_master, 
                container_is_up_master=container_is_up_master,
                location_type_master=location_type_master,
                service_name_master=service_name_master, 
                local_path_master=local_path_master,remote_path_master=remote_path_master, remote_user_master=remote_user_master,remote_host_master=remote_host_master,private_key_path_master=private_key_path_master, remote_password_master=remote_password_master,
                local_mysql_path_master=local_mysql_path_master, local_mysqldump_path_master=local_mysqldump_path_master,remote_mysql_path_master=remote_mysql_path_master, remote_mysqldump_path_master=remote_mysqldump_path_master,
                mysqlusername_slave=mysqlusername_slave, mysqlpassword_slave=mysqlpassword_slave, mysqlhost_slave=mysqlhost_slave, mysqlport_slave=mysqlport_slave,                  
                max_attempts_slave=max_attempts_slave, sleep_time_slave=sleep_time_slave, 
                container_is_up_slave=container_is_up_slave,
                location_type_slave=location_type_slave,
                service_name_slave=service_name_slave, 
                local_path_slave=local_path_slave,remote_path_slave=remote_path_slave, remote_user_slave=remote_user_slave,remote_host_slave=remote_host_slave,private_key_path_slave=private_key_path_slave, remote_password_slave=remote_password_slave,
                local_mysql_path_slave=local_mysql_path_slave, local_mysqldump_path_slave=local_mysqldump_path_slave,remote_mysql_path_slave=remote_mysql_path_slave, remote_mysqldump_path_slave=remote_mysqldump_path_slave)
    
    
    # 创建复制用户
    replication.db_master.create_user_and_grant_privileges(repl_user,repl_password)

    # 主数据库是否要导入数据
    if master_datacopying_is_required:
        replication.db_master.import_database('mydb',"/home/zym/web_meiduo_mall_docker/backend/mysql/master_db2.sql")
    # 数据导入从 master 到 slave
    if slave_datacopying_is_required:
        replication.import_data_from_db_master_to_db_slave()

    # 设置复制
    replication.set_up_replication()
    # 检查复制状态
    replication.check_replication_status()