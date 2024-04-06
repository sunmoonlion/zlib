import subprocess
import time
import yaml

class MySQLReplication:
    def __init__(self):
        self.load_config()

    def load_config(self):
        with open("../database/mysql/docker-compose.yml", "r") as config_file:
            self.config = yaml.safe_load(config_file)

    def start_databases(self):
        # 启动数据库
        subprocess.run(["docker-compose", "-f", "docker-compose.yml", "up", "-d"])

        # 等待主数据库启动完成
        if not self.wait_for_main_db_ready():
            print("Failed to start databases: Main database is not ready.")
            return

        print("Databases started successfully.")

    def wait_for_main_db_ready(self):
        # 等待主数据库启动完成
        max_attempts = 10
        for _ in range(max_attempts):
            time.sleep(5)  # 每隔5秒检查一次
            result = subprocess.run(["docker", "exec", self.config["services"]["main_db"]["container_name"], "mysql", "-u", "root", f"-p{self.config['services']['main_db']['environment']['MYSQL_ROOT_PASSWORD']}", "-e", "SELECT 1;"], capture_output=True)
            if result.returncode == 0:
                print("Main database is ready.")
                return True
        print("Main database is not ready after {} attempts.".format(max_attempts))
        return False

    def create_replication_user(self):
        subprocess.run(["docker", "exec", self.config["services"]["main_db"]["container_name"], "mysql", "-u", "root", f"-p{self.config['services']['main_db']['environment']['MYSQL_ROOT_PASSWORD']}", "-e",
                        f"CREATE USER IF NOT EXISTS '{self.config['services']['main_db']['environment']['MYSQL_USER']}'@'%' IDENTIFIED BY '{self.config['services']['main_db']['environment']['MYSQL_PASSWORD']}';"
                        f"GRANT REPLICATION SLAVE ON *.* TO '{self.config['services']['main_db']['environment']['MYSQL_USER']}'@'%';"
                        "FLUSH PRIVILEGES;"])

    def get_master_status(self):
        command = f"docker exec {self.config['services']['main_db']['container_name']} mysql -u root -p{self.config['services']['main_db']['environment']['MYSQL_ROOT_PASSWORD']} -e 'SHOW MASTER STATUS;'"
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        output = result.stdout.splitlines()[1].split("\t")
        binlog_file = output[0]
        binlog_position = output[1]
        return binlog_file, binlog_position

    def import_data_to_replica(self):
        subprocess.run(["docker", "exec", self.config["services"]["replica_db"]["container_name"], "mysql", "-u", "root", f"-p{self.config['services']['main_db']['environment']['MYSQL_ROOT_PASSWORD']}", "-e",
                        f"source /data/dump.sql"])

    def configure_replication(self, binlog_file, binlog_position):
        subprocess.run(["docker", "exec", self.config["services"]["replica_db"]["container_name"], "mysql", "-u", "root", f"-p{self.config['services']['replica_db']['environment']['MYSQL_ROOT_PASSWORD']}", "-e",
                        f"CHANGE MASTER TO MASTER_HOST='{self.config['services']['main_db']['container_name']}',"
                        f"MASTER_USER='{self.config['services']['main_db']['environment']['MYSQL_USER']}',"
                        f"MASTER_PASSWORD='{self.config['services']['main_db']['environment']['MYSQL_PASSWORD']}',"
                        f"MASTER_LOG_FILE='{binlog_file}',"
                        f"MASTER_LOG_POS={binlog_position};"])

    def start_replication(self):
        subprocess.run(["docker", "exec", self.config["services"]["replica_db"]["container_name"], "mysql", "-u", "root", f"-p{self.config['services']['replica_db']['environment']['MYSQL_ROOT_PASSWORD']}", "-e",
                        "START SLAVE;"])

    def check_replication_status(self):
        while True:
            result = subprocess.run(["docker", "exec", self.config["services"]["replica_db"]["container_name"], "mysql", "-u", "root", f"-p{self.config['services']['replica_db']['environment']['MYSQL_ROOT_PASSWORD']}", "-e",
                                     "SHOW SLAVE STATUS\G"], capture_output=True, text=True)
            if "Slave_IO_Running: Yes" in result.stdout and "Slave_SQL_Running: Yes" in result.stdout:
                print("Replication is running successfully.")
                break
            else:
                print("Replication is not yet running. Waiting...")
                time.sleep(5)

if __name__ == "__main__":
    replication = MySQLReplication()
    replication.start_databases()
    replication.create_replication_user()
    binlog_file, binlog_position = replication.get_master_status()
    # replication.import_data_to_replica()
    replication.configure_replication(binlog_file, binlog_position)
    replication.start_replication()
    replication.check_replication_status()
