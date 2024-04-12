import subprocess
import time
import yaml
import paramiko
import os
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
            try:
                self._ssh = paramiko.SSHClient()
                self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                self._ssh.connect(host, username=username, password=password)
            except paramiko.AuthenticationException as e:
                print(f"Failed to authenticate with host {host}: {str(e)}")
                raise
            except paramiko.SSHException as e:
                print(f"SSH connection to host {host} failed: {str(e)}")
                raise

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
                local_path = self._normalize_path(local_path,is_directory=True)
                remote_path = self._normalize_path(remote_path,is_directory=True)
                
                local_dirs = self._get_directory_structure(local_path)
                remote_folder = os.path.join(remote_path,os.path.basename(local_path[:-1]))
                
                self._create_remote_directory_structure(local_dirs, remote_folder)
                self._upload_directory(local_path, remote_folder)
            else:
                local_path = self._normalize_path(local_path,is_directory=False)
                remote_path = self._normalize_path(remote_path,is_directory=False)
                
                self.create_remote_directory(remote_path)
                remote_path = os.path.join(remote_path, os.path.basename(local_path))
                self._upload_file(local_path, remote_path)
                
            print("上传成功。")  
        except RuntimeError as e:
            print(f"上传失败：{str(e)}")
            raise e
        finally:
            self.ssh_singleton.close()

    def download(self, remote_path, local_path):
        try:
            self._establish_ssh_connection()
            ssh = self.ssh_singleton.get_ssh()
            sftp = ssh.open_sftp()

            try:
                if self._is_remote_directory(sftp, remote_path):
                    local_path = self._normalize_path(local_path, is_directory=True)
                    remote_path = self._normalize_path(remote_path, is_directory=True)
                    self._download_directory(sftp, remote_path, local_path)
                else:
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
        remote_filename = os.path.basename(remote_path)
        local_path = os.path.join(local_dir, remote_filename)
        sftp.get(remote_path, local_path)

    def _download_directory(self, sftp, remote_path, local_dir):
        files_and_directories = sftp.listdir_attr(remote_path)
        for item in files_and_directories:
            item_name = item.filename
            remote_item_path = os.path.join(remote_path, item_name)
            local_item_path = os.path.join(local_dir, item_name)
            if stat.S_ISDIR(item.st_mode):
                os.makedirs(local_item_path, exist_ok=True)
                self._download_directory(sftp, remote_item_path, local_item_path)
            else:
                self._download_file(sftp, remote_item_path, local_dir)
        

class Container:
    def __init__(self, local_path, remote_path, service_name=None, remote_host=None, remote_user=None, remote_password=None,
                 location_type='local', max_attempts=10, sleep_time=5):
        self.local_path = local_path
        self.remote_path = remote_path
        self.service_name = service_name
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.remote_password = remote_password
        self.location_type = location_type
        self.max_attempts = max_attempts
        self.sleep_time = sleep_time
        self.load_config()
        self.ssh_singleton = SSHSingleton()

    def _establish_ssh_connection(self):
        self.ssh_singleton.connect(self.remote_host, self.remote_user, self.remote_password)

    def load_file(self):
        load_file = FileTransfer(self.remote_host, self.remote_user, self.remote_password)
        load_file.upload(self.local_path, self.remote_path)
        
    def get_local_yml_path(self):
        for root, dirs, files in os.walk(self.local_path):
            for file in files:
                if file == "docker-compose.yml":
                    return os.path.join(root, file)
        raise FileNotFoundError("docker-compose.yml not found in the specified directory.")
       
  
    def get_remote_yml_path(self):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.remote_host, username=self.remote_user, password=self.remote_password)
        
        command = f"find {self.remote_path} -type f -name docker-compose.yml"
        stdin, stdout, stderr = ssh.exec_command(command)
        remote_yml_path = stdout.read().strip().decode('utf-8')
        
        ssh.close()
        return remote_yml_path

    
    def load_config(self):
        with open(self.get_local_yml_path(), "r") as config_file:
            self.config = yaml.safe_load(config_file)    
    
    def up_services(self):
        # 启动指定的服务或所有服务
        if self.service_name is None:
            self.up_all_services()
        elif isinstance(self.service_name, str):
            self.up_service(self.service_name)
        elif isinstance(self.service_name, list):
            if self.service_name:
                for name in self.service_name:
                    self.up_service(name)
            else:
                self.start_all_services()
        else:
            raise ValueError("Invalid service_name type. Must be a string or a list of strings or None.")

    def up_service(self, name):
        if self.location_type == 'remote':
            self.up_service_remote(name)
        elif self.location_type == 'local':
            self.up_service_local(name)
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")

    def up_service_remote(self, name):
        print(f"Starting {name} service...")
        command = f"cd {os.path.dirname(self.get_remote_yml_path())} && sudo docker-compose -f {self.get_remote_yml_path()} up -d {name}"
        self.execute_ssh_command(command)

        if name is not None and not self.wait_for_container_ready(name):
            print(f"Failed to start {name} database: Database is not ready.")
            return

    def up_service_local(self, name):
        print(f"Starting local {name} database...")
        subprocess.run(["sudo", "docker-compose", "-f", self.get_local_yml_path(), "up", "-d", name],
                       cwd=os.path.dirname(self.get_local_yml_path()))

        if name is not None and not self.wait_for_container_ready(name):
            print(f"Failed to start local {name} database: Database is not ready.")
            return

    def up_all_services(self):
        # 启动所有服务
        if self.location_type == 'remote':
            print("Starting all databases on remote host...")
            command = f"cd {self.remote_path} && sudo docker-compose -f {os.path.basename(self.get_local_yml_path())} up -d"
            self.execute_ssh_command(command)
        elif self.location_type == 'local':
           print("Starting all local databases...")
           subprocess.run(["sudo", "docker-compose", "-f", self.get_local_yml_path(), "up", "-d"],
                       cwd=os.path.dirname(self.get_local_yml_path()))
    
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")

    def wait_for_container_ready(self, name):
        print("Waiting for container to be ready...")
        for attempt in range(1, self.max_attempts + 1):
            print(f"Attempt {attempt}/{self.max_attempts}")
            time.sleep(self.sleep_time)
            if self.check_status(name, 'up'):
                print("Database is ready.")
                return True
            else:
                print("Database is not ready.")
        print("Database is not ready after {} attempts.".format(self.max_attempts))
        return False

    def check_status(self, name, action):
        if action == 'up':
            command = ["sudo", "docker", "ps", "-a", "--filter", f"name={name}", "--format", "{{.Names}}"]
        elif action == 'removed':
            command = ["sudo", "docker", "ps", "-a", "--filter", f"name={name}", "--format", "{{.Names}}"]
        elif action == 'stopped':
            command = ["sudo", "docker", "ps", "--filter", f"name={name}", "--format", "{{.Names}}"]
        else:
            raise ValueError("Invalid action. Must be 'up', 'removed' or 'stopped'.")
        
        if self.location_type == 'remote':
            stdout, stderr = self.execute_ssh_command(" ".join(command))
            if name in stdout:
                return True
        elif self.location_type == 'local':
            result = subprocess.run(command, capture_output=True, text=True)
            if result.returncode == 0 and name in result.stdout:
                return True
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")
        return False

    def execute_ssh_command(self, command):
        print("Executing SSH command:", command)
        self._establish_ssh_connection()
        ssh = self.ssh_singleton.get_ssh()
        stdin, stdout, stderr = ssh.exec_command(command)
        output = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')
        return output, error


    def stop_services(self):
        # 停止指定的服务或所有服务
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

    def stop_service(self, name):
        if self.location_type == 'remote':
            self.stop_service_remote(name)
        elif self.location_type == 'local':
            self.stop_service_local(name)
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")

    def stop_service_remote(self, name):
        print(f"Stopping {name} service...")
        command = f"cd {os.path.dirname(self.get_remote_yml_path())} && sudo docker-compose -f {self.get_remote_yml_path()} stop {name}"
        self.execute_ssh_command(command)

        if name is not None and not self.wait_for_container_stopped(name):
            print(f"Failed to stop {name} database.")

    def stop_service_local(self, name):
        print(f"Stopping local {name} database...")
        subprocess.run(["sudo", "docker-compose", "-f", self.get_local_yml_path(), "stop", name],
                       cwd=os.path.dirname(self.get_local_yml_path()))

        if name is not None and not self.wait_for_container_stopped(name):
            print(f"Failed to stop local {name} database.")

    def stop_all_services(self):
        # 停止所有服务
        if self.location_type == 'remote':
            print("Stopping all service on remote host...")
            command = f"cd {self.remote_path} && sudo docker-compose -f {os.path.basename(self.get_local_yml_path())} stop"
            self.execute_ssh_command(command)
        elif self.location_type == 'local':
            print("Stopping all local service...")
            subprocess.run(["sudo", "docker-compose", "-f", self.get_local_yml_path(), "stop"],
                           cwd=os.path.dirname(self.get_local_yml_path()))
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")

    def wait_for_container_stopped(self, name):
        print("Waiting for container to be stopped...")
        for attempt in range(1, self.max_attempts + 1):
            print(f"Attempt {attempt}/{self.max_attempts}")
            time.sleep(self.sleep_time)
            if not self.check_status(name,'stopped'):
                print("container is stopped.")
                return True
            else:
                print("container is still running.")
        print("Database is still running after {} attempts.".format(self.max_attempts))
        return False

    def down_services(self, remove_volumes=True):
        # 移除指定的服务或所有服务
        if self.service_name is None:
            self.down_all_services(remove_volumes=remove_volumes)
        elif isinstance(self.service_name, str):
            self.down_service(self.service_name, remove_volumes=remove_volumes)
        elif isinstance(self.service_name, list):
            if self.service_name:
                for name in self.service_name:
                    self.down_service(name, remove_volumes=remove_volumes)
            else:
                self.down_all_services(remove_volumes=remove_volumes)
        else:
            raise ValueError("Invalid service_name type. Must be a string or a list of strings or None.")

    def down_service(self, name, remove_volumes=True):
        if self.location_type == 'remote':
            self.down_service_remote(name, remove_volumes=remove_volumes)
        elif self.location_type == 'local':
            self.down_service_local(name, remove_volumes=remove_volumes)
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")

    def down_service_remote(self, name, remove_volumes=True):
        print(f"Removing {name} service containers...")
        # Remove the service containers
        command = (f"cd {os.path.dirname(self.get_remote_yml_path())} && "
                   f"sudo docker-compose -f {self.get_remote_yml_path()} down --remove-orphans {name}")
        self.execute_ssh_command(command)
        
        if remove_volumes:
            # Remove the volumes associated with the service
            print(f"Removing volumes associated with {name} service...")
            command = (f"sudo docker volume prune --filter label=com.docker.compose.project={name}")
            subprocess.run(command)

        if name is not None and not self.wait_for_container_removed(name):
            print(f"{name} service containers may not have been completely removed.")

    def down_service_local(self, name, remove_volumes=True):
        print(f"Removing local {name} database containers...")
        # Remove the service containers
        command = ["sudo", "docker-compose", "-f", self.get_local_yml_path(), "down", "--remove-orphans", name]
        self.execute_ssh_command(command)
        
        if remove_volumes:
            # Remove the volumes associated with the service
            print(f"Removing volumes associated with {name} service...")
            command = (f"sudo docker volume prune --filter label=com.docker.compose.project={name}")
            subprocess.run(command)

        if not self.wait_for_container_removed(name):
            print(f"Local {name} database containers may not have been completely removed.")

    def down_all_services(self, remove_volumes=True):
        if self.location_type == 'remote':
            print("Removing all databases on remote host...")
            command = f"cd {self.remote_path} && sudo docker-compose -f {os.path.basename(self.get_remote_yml_path())} down --remove-orphans"
            self.execute_ssh_command(command)
        elif self.location_type == 'local':
            print("Removing all local databases...")
            command = ["sudo", "docker-compose", "-f", self.get_local_yml_path(), "down", "--remove-orphans"]
            self.execute_ssh_command(command)
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")

    def wait_for_container_removed(self, name):
        print("Waiting for container to be removed...")
        for attempt in range(1, self.max_attempts + 1):
            print(f"Attempt {attempt}/{self.max_attempts}")
            time.sleep(self.sleep_time)
            if not self.check_status(name, 'removed'):
                print("container is removed.")
                return True
            else:
                print("container is still running.")
        print("Database is not ready after {} attempts.".format(self.max_attempts))
        return False

