import subprocess
import time
import yaml
import paramiko
import os


class StartContainer:
    def __init__(self, local_yml_path, service_name=None, remote_host=None, remote_user=None, remote_password=None,
                 location_type='local', remote_yml_dir=None, max_attempts=10, sleep_time=5):
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.remote_password = remote_password
        self.location_type = location_type
        self.local_yml_path = local_yml_path
        self.remote_yml_dir = remote_yml_dir
        self.service_name = service_name
        self.max_attempts = max_attempts
        self.sleep_time = sleep_time
        self.load_config()

    def load_config(self):
        # 加载本地YAML文件中的配置
        with open(self.local_yml_path, "r") as config_file:
            self.config = yaml.safe_load(config_file)

    def start_databases(self):
        # 启动指定的服务或所有服务
        if self.service_name is None:
            self.start_all_databases()
        elif isinstance(self.service_name, str):
            self.start_database(self.service_name)
        elif isinstance(self.service_name, list):
            if self.service_name:
                for name in self.service_name:
                    self.start_database(name)
            else:
                self.start_all_databases()
        else:
            raise ValueError("Invalid service_name type. Must be a string or a list of strings or None.")

    def start_database(self, name):
        # 启动特定的服务
        if self.location_type == 'remote':
            self.start_database_remote(name)
        elif self.location_type == 'local':
            self.start_database_local(name)
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")

    def start_database_remote(self, name):
        # 在远程主机上启动特定的服务
        print("Creating directory on remote host...")
        self.execute_ssh_command(f"mkdir -p {self.remote_yml_dir}")

        print("Uploading docker-compose.yml file...")
        self.upload_file(self.local_yml_path, os.path.join(self.remote_yml_dir, os.path.basename(self.local_yml_path)))

        print(f"Starting {name} database...")
        command = f"cd {self.remote_yml_dir} && sudo docker-compose -f {os.path.basename(self.local_yml_path)} up -d {name}"
        self.execute_ssh_command(command)

        if name is not None and not self.wait_for_db_ready(name):
            print(f"Failed to start {name} database: Database is not ready.")
            return

    def start_database_local(self, name):
        # 在本地启动特定的服务
        print(f"Starting local {name} database...")
        subprocess.run(["sudo", "docker-compose", "-f", self.local_yml_path, "up", "-d", name],
                       cwd=os.path.dirname(self.local_yml_path))

        if name is not None and not self.wait_for_db_ready(name):
            print(f"Failed to start local {name} database: Database is not ready.")
            return

    def start_all_databases(self):
        # 启动所有服务
        if self.location_type == 'remote':
            print("Starting all databases on remote host...")
            command = f"cd {self.remote_yml_dir} && sudo docker-compose -f {os.path.basename(self.local_yml_path)} up -d"
            self.execute_ssh_command(command)
        elif self.location_type == 'local':
            print("Starting all local databases...")
            subprocess.run(["sudo", "docker-compose", "-f", self.local_yml_path, "up", "-d"],
                           cwd=os.path.dirname(self.local_yml_path))
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")

    def wait_for_db_ready(self, name):
        # 等待服务就绪
        print("Waiting for database to be ready...")
        for attempt in range(1, self.max_attempts + 1):
            print(f"Attempt {attempt}/{self.max_attempts}")
            time.sleep(self.sleep_time)
            if self.check_database(name):
                print("Database is ready.")
                return True
            else:
                print("Database is not ready.")
        print("Database is not ready after {} attempts.".format(self.max_attempts))
        return False

    def check_database(self, name):
        # 检查服务是否运行
        container_name = self.config['services'][name]['container_name']
        if self.location_type == 'remote':
            stdout, stderr = self.execute_ssh_command(f"sudo docker ps | grep {container_name} | awk '{{print $NF}}'")
            if container_name in stdout:
                return True
        elif self.location_type == 'local':
            result = subprocess.run(
                ["sudo", "docker", "ps", "--filter", f"name={container_name}", "--format", "{{.Names}}"],
                capture_output=True, text=True)
            if result.returncode == 0 and container_name in result.stdout:
                return True
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")
        return False

    def execute_ssh_command(self, command):
        # 在远程主机上执行SSH命令
        print("Executing SSH command:", command)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.remote_host, username=self.remote_user, password=self.remote_password)
        stdin, stdout, stderr = ssh.exec_command(command)
        output = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')
        ssh.close()
        return output, error

    def upload_file(self, local_path, remote_path):
        # 通过SSH上传文件到远程主机
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.remote_host, username=self.remote_user, password=self.remote_password)
        sftp = ssh.open_sftp()
        sftp.put(local_path, remote_path)
        sftp.close()
        ssh.close()


class StopContainer:
    def __init__(self, local_yml_path, service_name=None, remote_host=None, remote_user=None, remote_password=None,
                 location_type='local', remote_yml_dir=None, max_attempts=10, sleep_time=5):
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.remote_password = remote_password
        self.location_type = location_type
        self.local_yml_path = local_yml_path
        self.remote_yml_dir = remote_yml_dir
        self.service_name = service_name
        self.max_attempts = max_attempts
        self.sleep_time = sleep_time
        self.load_config()

    def load_config(self):
        # 加载本地YAML文件中的配置
        with open(self.local_yml_path, "r") as config_file:
            self.config = yaml.safe_load(config_file)


    def stop_services(self):
        try:
            if self.location_type == 'remote':
                self.upload_file(self.local_yml_path, os.path.join(self.remote_yml_dir, os.path.basename(self.local_yml_path)))
            if self.service_name is None:
                self.stop_all_services()
            elif isinstance(self.service_name, str):
                self.stop_service(self.service_name)
            elif isinstance(self.service_name, list):
                if self.service_name:
                    for name in self.service_name:
                        self.stop_service(name)
                else:
                    self.stop_all_services()
            else:
                raise ValueError("Invalid service_name type. Must be a string or a list of strings or None.")
        except Exception as e:
            print(f"Error occurred while stopping services: {str(e)}")

    def stop_service(self, name):
        try:
            if self.location_type == 'remote':
                self.stop_service_remote(name)
            elif self.location_type == 'local':
                self.stop_service_local(name)
            else:
                raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")

            # 等待服务关闭
            if not self.wait_for_service_stopped(name):
                print(f"Failed to stop {name} service: Service is still running.")
        except Exception as e:
            print(f"Error occurred while stopping {name} service: {str(e)}")

    def stop_service_remote(self, name):
        # 在远程主机上停止特定的服务
        print("Creating directory on remote host...")
        self.execute_ssh_command(f"mkdir -p {self.remote_yml_dir}")

        print("Uploading docker-compose.yml file...")
        self.upload_file(self.local_yml_path, os.path.join(self.remote_yml_dir, os.path.basename(self.local_yml_path)))

        print(f"Stopping {name} service on remote host...")
        command = f"cd {self.remote_yml_dir} && sudo docker-compose -f {os.path.basename(self.local_yml_path)} stop {name}"
        self.execute_ssh_command(command)

    def stop_service_local(self, name):
        print(f"Stopping local {name} service...")
        subprocess.run(["sudo", "docker-compose", "-f", self.local_yml_path, "stop", name],
                       cwd=os.path.dirname(self.local_yml_path))

    def stop_all_services(self):
        try:
            if self.location_type == 'remote':
                print("Stopping all services on remote host...")
                command = f"cd {self.remote_yml_dir} && sudo docker-compose -f {os.path.basename(self.local_yml_path)} stop"
                self.execute_ssh_command(command)
            elif self.location_type == 'local':
                print("Stopping all local services...")
                subprocess.run(["sudo", "docker-compose", "-f", self.local_yml_path, "stop"],
                               cwd=os.path.dirname(self.local_yml_path))
            else:
                raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")
        except Exception as e:
            print(f"Error occurred while stopping all services: {str(e)}")

    def wait_for_service_stopped(self, name):
        # 等待服务停止
        print(f"Waiting for {name} service to be stopped...")
        for attempt in range(1, self.max_attempts + 1):
            print(f"Attempt {attempt}/{self.max_attempts}")
            time.sleep(self.sleep_time)
            if not self.check_service_running(name):
                print(f"{name} service is stopped.")
                return True
            else:
                print(f"{name} service is still running.")
        print(f"{name} service is still running after {self.max_attempts} attempts.")
        return False

    def check_service_running(self, name):
        # 检查服务是否在运行
        container_name = self.config['services'][name]['container_name']
        if self.location_type == 'remote':
            stdout, stderr = self.execute_ssh_command(f"sudo docker ps | grep {container_name} | awk '{{print $NF}}'")
            if container_name in stdout:
                return True
        elif self.location_type == 'local':
            result = subprocess.run(
                ["sudo", "docker", "ps", "--filter", f"name={container_name}", "--format", "{{.Names}}"],
                capture_output=True, text=True)
            if result.returncode == 0 and container_name in result.stdout:
                return True
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")
        return False

    def execute_ssh_command(self, command):
        print("Executing SSH command:", command)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.remote_host, username=self.remote_user, password=self.remote_password)
        stdin, stdout, stderr = ssh.exec_command(command)
        output = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')
        ssh.close()
        return output, error

    def upload_file(self, local_path, remote_path):
        print("Uploading docker-compose.yml file to remote host...")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.remote_host, username=self.remote_user, password=self.remote_password)
        sftp = ssh.open_sftp()
        sftp.put(local_path, remote_path)
        sftp.close()
        ssh.close()


class DownContainer:
    def __init__(self, local_yml_path, service_name=None, remote_host=None, remote_user=None, remote_password=None,
                 location_type='local', remote_yml_dir=None, max_attempts=10, sleep_time=5):
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.remote_password = remote_password
        self.location_type = location_type
        self.local_yml_path = local_yml_path
        self.remote_yml_dir = remote_yml_dir
        self.service_name = service_name
        self.max_attempts = max_attempts
        self.sleep_time = sleep_time
        self.load_config()

    def load_config(self):
        # 加载本地YAML文件中的配置
        with open(self.local_yml_path, "r") as config_file:
            self.config = yaml.safe_load(config_file)


    def down_containers(self):
        try:
            if self.location_type == 'remote':
                self.upload_file(self.local_yml_path, os.path.join(self.remote_yml_dir, os.path.basename(self.local_yml_path)))
            if self.service_name is None:
                self.down_all_containers()
            elif isinstance(self.service_name, str):
                self.down_container(self.service_name)
            elif isinstance(self.service_name, list):
                if self.service_name:
                    for name in self.service_name:
                        self.down_container(name)
                else:
                    self.down_all_containers()
            else:
                raise ValueError("Invalid service_name type. Must be a string or a list of strings or None.")
        except Exception as e:
            print(f"Error occurred while stopping and removing containers: {str(e)}")

    def down_container(self, name):
        try:
            if self.location_type == 'remote':
                self.down_container_remote(name)
            elif self.location_type == 'local':
                self.down_container_local(name)
            else:
                raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")

            # 等待容器停止和移除
            if not self.wait_for_container_removed(name):
                print(f"Failed to stop and remove {name} container.")
        except Exception as e:
            print(f"Error occurred while stopping and removing {name} container: {str(e)}")

    def down_container_remote(self, name):
        # 在远程主机上停止和移除特定容器
        print("Creating directory on remote host...")
        self.execute_ssh_command(f"mkdir -p {self.remote_yml_dir}")

        print("Uploading docker-compose.yml file...")
        self.upload_file(self.local_yml_path, os.path.join(self.remote_yml_dir, os.path.basename(self.local_yml_path)))

        print(f"Stopping and removing {name} container on remote host...")
        command = f"cd {self.remote_yml_dir} && sudo docker-compose -f {os.path.basename(self.local_yml_path)} down --remove-orphans {name}"
        self.execute_ssh_command(command)

    def down_container_local(self, name):
        print(f"Stopping and removing local {name} container...")
        subprocess.run(["sudo", "docker-compose", "-f", self.local_yml_path, "down", "--remove-orphans", name],
                       cwd=os.path.dirname(self.local_yml_path))

    def down_all_containers(self):
        try:
            if self.location_type == 'remote':
                print("Stopping and removing all containers on remote host...")
                command = f"cd {self.remote_yml_dir} && sudo docker-compose -f {os.path.basename(self.local_yml_path)} down --remove-orphans"
                self.execute_ssh_command(command)
            elif self.location_type == 'local':
                print("Stopping and removing all local containers...")
                subprocess.run(["sudo", "docker-compose", "-f", self.local_yml_path, "down", "--remove-orphans"],
                               cwd=os.path.dirname(self.local_yml_path))
            else:
                raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")
        except Exception as e:
            print(f"Error occurred while stopping and removing all containers: {str(e)}")

    def wait_for_container_removed(self, name):
        # 等待容器被停止和移除
        print(f"Waiting for {name} container to be stopped and removed...")
        for attempt in range(1, self.max_attempts + 1):
            print(f"Attempt {attempt}/{self.max_attempts}")
            time.sleep(self.sleep_time)
            if not self.check_container_exists(name):
                print(f"{name} container is stopped and removed.")
                return True
            else:
                print(f"{name} container is still running.")
        print(f"{name} container is still running after {self.max_attempts} attempts.")
        return False

    def check_container_exists(self, name):
        # 检查容器是否存在
        container_name = self.config['services'][name]['container_name']
        if self.location_type == 'remote':
            stdout, stderr = self.execute_ssh_command(f"sudo docker ps -a | grep {container_name} | awk '{{print $NF}}'")
            if container_name in stdout:
                return True
        elif self.location_type == 'local':
            result = subprocess.run(
                ["sudo", "docker", "ps", "-a", "--filter", f"name={container_name}", "--format", "{{.Names}}"],
                capture_output=True, text=True)
            if result.returncode == 0 and container_name in result.stdout:
                return True
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")
        return False

    def execute_ssh_command(self, command):
        print("Executing SSH command:", command)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.remote_host, username=self.remote_user, password=self.remote_password)
        stdin, stdout, stderr = ssh.exec_command(command)
        output = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')
        ssh.close()
        return output, error

    def upload_file(self, local_path, remote_path):
        print("Uploading docker-compose.yml file to remote host...")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.remote_host, username=self.remote_user, password=self.remote_password)
        sftp = ssh.open_sftp()
        sftp.put(local_path, remote_path)
        sftp.close()
        ssh.close()


class RemoveVolume:
    def __init__(self, local_yml_path, service_name=None, remote_host=None, remote_user=None, remote_password=None,
                 location_type='local', remote_yml_dir=None):
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.remote_password = remote_password
        self.location_type = location_type
        self.local_yml_path = local_yml_path
        self.remote_yml_dir = remote_yml_dir
        self.service_name = service_name

    def remove_volumes(self):
        if self.service_name is None:
            self.remove_all_volumes()
        elif isinstance(self.service_name, str):
            self.remove_volume(self.service_name)
        elif isinstance(self.service_name, list):
            if self.service_name:
                for name in self.service_name:
                    self.remove_volume(name)
            else:
                self.remove_all_volumes()
        else:
            raise ValueError("Invalid service_name type. Must be a string or a list of strings or None.")

    def remove_volume(self, service_name):
        volume_name = self.get_volume_name(service_name)
        if self.location_type == 'remote':
            self.remove_volume_remote(volume_name)
        elif self.location_type == 'local':
            self.remove_volume_local(volume_name)
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")

    def get_volume_name(self, service_name):
        with open(self.local_yml_path, "r") as config_file:
            config = yaml.safe_load(config_file)
            volumes = config['services'][service_name]['volumes']
            if volumes:
                return volumes[0].split(':')[0]
            else:
                raise ValueError("No volumes defined for the service.")

    def remove_volume_remote(self, volume_name):
        command = f"docker volume rm {volume_name}"
        self.execute_ssh_command(command)

    def remove_volume_local(self, volume_name):
        command = f"docker volume rm {volume_name}"
        subprocess.run(command, shell=True)

    def remove_all_volumes(self):
        print("Removing all volumes...")
        if self.location_type == 'remote':
            command = "docker volume ls --format '{{.Name}}' | grep -v '^$'"
            volumes = self.execute_ssh_command(command)[0].split('\n')
            for volume in volumes:
                if volume:
                    self.remove_volume_remote(volume)
        elif self.location_type == 'local':
            command = "docker volume ls --format '{{.Name}}' | grep -v '^$'"
            volumes = subprocess.run(command, shell=True, capture_output=True, text=True).stdout.split('\n')
            for volume in volumes:
                if volume:
                    self.remove_volume_local(volume)
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")

    def execute_ssh_command(self, command):
        print("Executing SSH command:", command)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.remote_host, username=self.remote_user, password=self.remote_password)
        stdin, stdout, stderr = ssh.exec_command(command)
        output = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')
        ssh.close()
        return output, error


if __name__ == "__main__":
    # 定义远程主机和凭据
    remote_host = "47.103.135.26"
    remote_user = "zym"
    remote_password = "alyfwqok"

    # 定义本地YAML文件路径和远程YAML目录
    local_yml_path = "/home/WUYING_13701819268_15611880/Desktop/docker-compose.yml"
    remote_yml_dir = "/home/zym/test1"

    # 定义单个服务名称
    service_name_single = "service1"
    
    # 定义多个服务名称
    service_names_batch_local = ["service1", "service2", "service3"]
    
    # 创建本地实例并启动单个服务
    db_local_single = StartContainer(local_yml_path=local_yml_path, service_name=service_name_single)
    db_local_single.start_databases()

    # 创建远程实例并启动单个服务
    db_remote_single = StartContainer(local_yml_path=local_yml_path, service_name=service_name_single,
                                      remote_host=remote_host, remote_user=remote_user, remote_password=remote_password,
                                      location_type='remote', remote_yml_dir=remote_yml_dir)
    db_remote_single.start_databases()

    # 创建本地实例并启动批量服务
    db_local_batch = StartContainer(local_yml_path=local_yml_path, service_name=service_names_batch_local)
    db_local_batch.start_databases()

    # 创建远程实例并启动批量服务
    db_remote_batch = StartContainer(local_yml_path=local_yml_path, service_name=service_names_batch_local,
                                     remote_host=remote_host, remote_user=remote_user, remote_password=remote_password,
                                     location_type='remote', remote_yml_dir=remote_yml_dir)
    db_remote_batch.start_databases()

    # 创建不指定服务名称的实例以启动所有服务
    db_local_all = StartContainer(local_yml_path=local_yml_path)
    db_local_all.start_databases()

    # 创建不指定服务名称的远程实例以启动所有服务
    db_remote_all = StartContainer(local_yml_path=local_yml_path, remote_host=remote_host, remote_user=remote_user,
                                    remote_password=remote_password, location_type='remote', remote_yml_dir=remote_yml_dir)
    db_remote_all.start_databases()
    
    
    # 创建本地实例并停止单个服务
    stop_db_local_single = StopContainer(local_yml_path=local_yml_path, service_name=service_name_single)
    stop_db_local_single.stop_services()

    # 创建远程实例并停止单个服务
    stop_db_remote_single = StopContainer(local_yml_path=local_yml_path, service_name=service_name_single,
                                          remote_host=remote_host, remote_user=remote_user, remote_password=remote_password,
                                          location_type='remote', remote_yml_dir=remote_yml_dir)
    stop_db_remote_single.stop_services()

    # 创建本地实例并停止批量服务
    stop_db_local_batch = StopContainer(local_yml_path=local_yml_path, service_name=service_names_batch_local)
    stop_db_local_batch.stop_services()

    # 创建远程实例并停止批量服务
    stop_db_remote_batch = StopContainer(local_yml_path=local_yml_path, service_name=service_names_batch_local,
                                          remote_host=remote_host, remote_user=remote_user, remote_password=remote_password,
                                          location_type='remote', remote_yml_dir=remote_yml_dir)
    stop_db_remote_batch.stop_services()

    # 创建不指定服务名称的本地实例以停止所有服务
    stop_db_local_all = StopContainer(local_yml_path=local_yml_path)
    stop_db_local_all.stop_services()

    # 创建不指定服务名称的远程实例以停止所有服务
    stop_db_remote_all = StopContainer(local_yml_path=local_yml_path, remote_host=remote_host, remote_user=remote_user,
                                        remote_password=remote_password, location_type='remote', remote_yml_dir=remote_yml_dir)
    stop_db_remote_all.stop_services()


    # 创建本地实例并移除单个服务
    down_db_local_single = DownContainer(local_yml_path=local_yml_path, service_name=service_name_single)
    down_db_local_single.down_containers()

    # 创建远程实例并移除单个服务
    down_db_remote_single = DownContainer(local_yml_path=local_yml_path, service_name=service_name_single,
                                          remote_host=remote_host, remote_user=remote_user, remote_password=remote_password,
                                          location_type='remote', remote_yml_dir=remote_yml_dir)
    down_db_remote_single.down_containers()

    # 创建本地实例并移除批量服务
    down_db_local_batch = DownContainer(local_yml_path=local_yml_path, service_name=service_names_batch_local)
    down_db_local_batch.down_containers()

    # 创建远程实例并移除批量服务
    down_db_remote_batch = DownContainer(local_yml_path=local_yml_path, service_name=service_names_batch_local,
                                          remote_host=remote_host, remote_user=remote_user, remote_password=remote_password,
                                          location_type='remote', remote_yml_dir=remote_yml_dir)
    down_db_remote_batch.down_containers()

    # 创建不指定服务名称的本地实例以移除所有服务
    down_db_local_all = DownContainer(local_yml_path=local_yml_path)
    down_db_local_all.down_containers()

    # 创建不指定服务名称的远程实例以移除所有服务
    down_db_remote_all = DownContainer(local_yml_path=local_yml_path, remote_host=remote_host, remote_user=remote_user,
                                        remote_password=remote_password, location_type='remote', remote_yml_dir=remote_yml_dir)
    down_db_remote_all.down_containers()
    
    
    # 创建本地实例并移除单个服务的挂载卷
    volume_local_single = RemoveVolume(local_yml_path=local_yml_path, service_name="service1")
    volume_local_single.remove_volumes()

    # 创建远程实例并移除单个服务的挂载卷
    volume_remote_single = RemoveVolume(local_yml_path=local_yml_path, service_name="service1",
                                        remote_host=remote_host, remote_user=remote_user, remote_password=remote_password,
                                        location_type='remote')
    volume_remote_single.remove_volumes()

    # 创建本地实例并移除多个服务的挂载卷
    volume_local_batch = RemoveVolume(local_yml_path=local_yml_path, service_name=["service1", "service2", "service3"])
    volume_local_batch.remove_volumes()

    # 创建远程实例并移除多个服务的挂载卷
    volume_remote_batch = RemoveVolume(local_yml_path=local_yml_path, service_name=["service1", "service2", "service3"],
                                        remote_host=remote_host, remote_user=remote_user, remote_password=remote_password,
                                        location_type='remote')
    volume_remote_batch.remove_volumes()

    # 创建本地实例并移除所有服务的挂载卷
    volume_local_all = RemoveVolume(local_yml_path=local_yml_path)
    volume_local_all.remove_volumes()

    # 创建远程实例并移除所有服务的挂载卷
    volume_remote_all = RemoveVolume(local_yml_path=local_yml_path, remote_host=remote_host, remote_user=remote_user,
                                    remote_password=remote_password, location_type='remote')
    volume_remote_all.remove_volumes()
