import subprocess
import time
import yaml
import paramiko

class MySQLReplication:
    def __init__(self, remote_host, remote_user, remote_password):
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.remote_password = remote_password
        self.load_config()

    def load_config(self):
        with open("../../docker-compose.yml", "r") as config_file:
            self.config = yaml.safe_load(config_file)

    def execute_ssh_command(self, command):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.remote_host, username=self.remote_user, password=self.remote_password)
        stdin, stdout, stderr = ssh.exec_command(command)
        return stdout.readlines(), stderr.readlines()

    def start_databases(self):
        # 在远程机器上创建目标目录（如果目录不存在）
        print("Creating directory on remote host...")
        stdout, stderr = self.execute_ssh_command("mkdir -p /home/zym/test")
        print("STDOUT:", stdout)
        print("STDERR:", stderr)

        # 上传 docker-compose.yml 文件到远程计算机
        print("Uploading docker-compose.yml file...")
        self.upload_file("../../docker-compose.yml", "/home/zym/test/docker-compose.yml")

        # 切换到包含 docker-compose.yml 文件的目录并启动主数据库
        print('Starting main database...')
        command = "cd /home/zym/test && sudo docker-compose -f docker-compose.yml up -d main_db"
        stdout, stderr = self.execute_ssh_command(command)
        print("STDOUT:", stdout)
        print("STDERR:", stderr)

        # 检查主数据库是否已准备就绪
        if not self.wait_for_main_db_ready():
            print("Failed to start databases: Main database is not ready.")
            return

        # 启动从数据库
        print("Starting replica database...")
        stdout, stderr = self.execute_ssh_command("sudo docker-compose -f /home/zym/test/docker-compose.yml up -d replica_db")
        print("STDOUT:", stdout)
        print("STDERR:", stderr)

        # 检查从数据库是否已准备就绪
        if not self.wait_for_replica_db_ready():
            print("Failed to start databases: Replica database is not ready.")
            return

    def wait_for_main_db_ready(self):
        # 等待主数据库启动完成
        print("Waiting for main database to be ready...")
        max_attempts = 10
        for attempt in range(1, max_attempts + 1):
            print(f"Attempt {attempt}/{max_attempts}")
            time.sleep(5)
            result, _ = self.execute_ssh_command(
                f"mysql -u root -p{self.config['services']['main_db']['environment']['MYSQL_ROOT_PASSWORD']} -h {self.remote_host} -e 'SHOW DATABASES;'")
            print("Result:", result)
            if any("mysql" in line for line in result):
                print("Main database is ready.")
                return True
        print("Main database is not ready after {} attempts.".format(max_attempts))
        return False

    def wait_for_replica_db_ready(self):
        # 等待从数据库启动完成
        max_attempts = 20
        wait_time = 10
        for attempt in range(1, max_attempts + 1):
            print(f"Attempt {attempt}/{max_attempts}")
            time.sleep(wait_time)
            result, _ = self.execute_ssh_command(
                f"mysql -u root -p{self.config['services']['replica_db']['environment']['MYSQL_ROOT_PASSWORD']} -h {self.remote_host} -e 'SELECT 1;'")
            if "1" in "".join(result):
                print("Replica database is ready.")
                return True
        print("Replica database is not ready after {} attempts.".format(max_attempts))
        return False

    def upload_file(self, local_path, remote_path):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.remote_host, username=self.remote_user, password=self.remote_password)

        sftp = ssh.open_sftp()
        sftp.put(local_path, remote_path)
        sftp.close()
        ssh.close()


if __name__ == "__main__":
    # 设置远程主机的连接信息
    remote_host = "47.103.135.26"
    remote_user = "zym"
    remote_password = "alyfwqok"

    # 创建 MySQLReplication 实例并启动数据库
    replication = MySQLReplication(remote_host, remote_user, remote_password)
    replication.start_databases()