import subprocess
import time
import yaml
import paramiko
import stat
from concurrent.futures import ThreadPoolExecutor

class SSHSingleton:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._ssh = None
        return cls._instance

    def connect(self, host, username, password):
        if self._ssh is None or not self._ssh.get_transport().is_active():
            self._ssh = paramiko.SSHClient()
            self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self._ssh.connect(host, username=username, password=password)

    def get_ssh(self):
        return self._ssh

    def close(self):
        if self._ssh is not None:
            self._ssh.close()
            self._ssh = None

class FileTransfer: 
    def __init__(self, remote_host, remote_user, remote_password):
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.remote_password = remote_password
        self.ssh_singleton = SSHSingleton()

    def _establish_ssh_connection(self):
        self.ssh_singleton.connect(self.remote_host, self.remote_user, self.remote_password)

    def _normalize_path(self, path, is_directory):
        # 规范化路径，确保以斜杠结尾（对目录）或不以斜杠结尾（对文件）
        normalized_path = os.path.normpath(path)
        if is_directory:
            if not normalized_path.endswith('/'):
                normalized_path += '/'
        else:
            if normalized_path.endswith('/'):
                normalized_path = normalized_path[:-1]
        return normalized_path


    def create_remote_directory(self, remote_path):
        self._establish_ssh_connection()
        ssh = self.ssh_singleton.get_ssh()
        stdin, stdout, stderr = ssh.exec_command(f"mkdir -p {remote_path}")
        if stderr.read().strip():
            raise RuntimeError(f"Failed to create remote directory {remote_path}")

    def _get_directory_structure(self, dir_path):
        dirs = [] 
        for root, _, _ in os.walk(dir_path):
            dirs.append(os.path.relpath(root, dir_path))
        return dirs

    def _create_remote_directory_structure(self, local_dirs, remote_path):
        self._establish_ssh_connection()
        ssh = self.ssh_singleton.get_ssh()
        for dir in local_dirs:
            remote_dir = os.path.join(remote_path, dir)
            self.create_remote_directory(remote_dir)
            
    def _upload_file(self, local_file_path, remote_file_path):
        self._establish_ssh_connection()
        ssh = self.ssh_singleton.get_ssh()
        sftp = ssh.open_sftp()
        try:
            sftp.put(local_file_path, remote_file_path)
        finally:
            sftp.close()

    def _upload_directory(self, local_path, remote_path):
        self._establish_ssh_connection()
        ssh = self.ssh_singleton.get_ssh()
        sftp = ssh.open_sftp()
        try:
            with ThreadPoolExecutor(max_workers=5) as executor:
                for root, dirs, files in os.walk(local_path):
                    for file in files:
                        local_file_path = os.path.join(root, file)
                        remote_file_path = os.path.join(remote_path, os.path.relpath(local_file_path, local_path))
                        executor.submit(self._upload_file, local_file_path, remote_file_path)
        finally:
            sftp.close()

    def _is_remote_directory(self, sftp, remote_path):
        try:
            return stat.S_ISDIR(sftp.stat(remote_path).st_mode)
        except IOError:
            return False
        
    def upload(self, local_path, remote_path):
        try:
            if os.path.isdir(local_path):
                # 规范化本地和远程路径
                local_path = self._normalize_path(local_path,is_directory=True)
                remote_path = self._normalize_path(remote_path,is_directory=True)
                
                #处理本地上传目录的结构，便于在远程创建相同的目录结构           
                local_dirs = self._get_directory_structure(local_path)
                #当本地上传的是目录时，我们将该目录整体上传，因此，传入的远程目录路径要和本地上传的目录名作拼接
                remote_folder = os.path.join(remote_path,os.path.basename(local_path[:-1]))
                
                #创建一系列的远程目录（与传入的本地文件夹的目录结构相同）            
                self._create_remote_directory_structure(local_dirs, remote_folder)
                #上传所有文件
                self._upload_directory(local_path, remote_folder)
            else:
                # 规范化本地和远程路径
                local_path = self._normalize_path(local_path,is_directory=False)
                remote_path = self._normalize_path(remote_path,is_directory=False)
                #创建远程目录
                self.create_remote_directory(remote_path)
                #创建远程路径（包含文件名）
                remote_path = os.path.join(remote_path, os.path.basename(local_path))
                #上传文件
                self._upload_file(local_path, remote_path)
                
            print("上传成功。")  
        except RuntimeError as e:
            print(f"上传失败：{str(e)}")
            raise e
        finally:
            self.ssh_singleton.close()

    def download(self, remote_path, local_path):
        try:
            # 下载文件或目录
            self._establish_ssh_connection()
            ssh = self.ssh_singleton.get_ssh()
            sftp = ssh.open_sftp()

            try:
                # 判断远程路径是文件还是目录
                if self._is_remote_directory(sftp, remote_path):
                    
                    # 如果是目录，先规范化路径
                    local_path = self._normalize_path(local_path, is_directory=True)
                    remote_path = self._normalize_path(remote_path, is_directory=True)
                    # 然后下载目录及其内容
                    self._download_directory(sftp, remote_path, local_path)
                else:
                    # 如果是文件，直接下载
                    self._download_file(sftp, remote_path, local_path)
                    print("下载成功。")
            finally:
                sftp.close()
        except RuntimeError as e:
            print(f"下载失败：{str(e)}")
            raise e
        finally:
            self.ssh_singleton.close()

    def _download_file(self, sftp, remote_path, local_dir):
        # 获取远程文件名
        remote_filename = os.path.basename(remote_path)
        # 构建本地文件路径
        local_path = os.path.join(local_dir, remote_filename)
        # 下载单个文件
        sftp.get(remote_path, local_path)


    def _download_directory(self, sftp, remote_path, local_dir):
        # 获取远程文件夹下的文件和子目录
        files_and_directories = sftp.listdir_attr(remote_path)
        for item in files_and_directories:
            item_name = item.filename
            remote_item_path = os.path.join(remote_path, item_name)
            local_item_path = os.path.join(local_dir, item_name)
            # 如果是目录，则递归下载
            if stat.S_ISDIR(item.st_mode):
                # 创建本地目录
                os.makedirs(local_item_path, exist_ok=True)
                # 递归下载子目录
                self._download_directory(sftp, remote_item_path, local_item_path)
            else:
                # 如果是文件，则下载到指定本地目录
                self._download_file(sftp, remote_item_path, local_dir)



class Container:
    def __init__(self, local_path, service_name=None, remote_host=None, remote_user=None, remote_password=None,
                 location_type='local', remote_path=None, max_attempts=10, sleep_time=5):
        self.local_path = local_path
        self.remote_path = remote_path
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

    def load_file(self):
        # 上传文件或文件夹
        load_file = FileTransfer(self.remote_host, self.remote_user, self.remote_password)
        load_file.upload(self.local_path, self.remote_item)
        
    def get_local_yml_path(self):
        pass
    
    def load_config(self):
        # 加载本地YAML文件中的配置
        with open(self.local_yml_path, "r") as config_file:
            self.config = yaml.safe_load(config_file)    
    
    def up_services(self):
        # 启动指定的服务或所有服务
        if self.service_name is None:
            self.up_all_databases()
        elif isinstance(self.service_name, str):
            self.up_database(self.service_name)
        elif isinstance(self.service_name, list):
            if self.service_name:
                for name in self.service_name:
                    self.up_database(name)
            else:
                self.start_all_databases()
        else:
            raise ValueError("Invalid service_name type. Must be a string or a list of strings or None.")

    def up_service(self, name):
        # 启动特定的服务
        if self.location_type == 'remote':
            self.up_database_remote(name)
        elif self.location_type == 'local':
            self.up_database_local(name)
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")

    def up_service_remote(self, name):
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

    def up_service_local(self, name):
        # 在本地启动特定的服务
        print(f"Starting local {name} database...")
        subprocess.run(["sudo", "docker-compose", "-f", self.local_yml_path, "up", "-d", name],
                       cwd=os.path.dirname(self.local_yml_path))

        if name is not None and not self.wait_for_db_ready(name):
            print(f"Failed to start local {name} database: Database is not ready.")
            return

    def up_all_services(self):
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

    def wait_for_container_ready(self, name):
        # 等待服务就绪
        print("Waiting for container to be ready...")
        for attempt in range(1, self.max_attempts + 1):
            print(f"Attempt {attempt}/{self.max_attempts}")
            time.sleep(self.sleep_time)
            if self.check_container(name):
                print("Database is ready.")
                return True
            else:
                print("Database is not ready.")
        print("Database is not ready after {} attempts.".format(self.max_attempts))
        return False

    def check_container(self, name):
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
    remote_host = "47.103.135.26"
    remote_user = "zym"
    remote_password = "alyfwqok"

    local_path = "/home/WUYING_13701819268_15611880/Desktop/web_meiduo_mall_docker"
    remote_path = "/home/zym/container"

    service_name_single = "db_master"
    
    db_local_single = Container(local_path=local_path, remote_path=remote_path,service_name=service_name_single,
                                      remote_host=remote_host, remote_user=remote_user, remote_password=remote_password)
    db_local_single.up_services()

    db_remote_single = Container(local_path=local_path, remote_path=remote_path,service_name=service_name_single,
                                      remote_host=remote_host, remote_user=remote_user, remote_password=remote_password,
                                      location_type='remote')
    db_remote_single.up_services()