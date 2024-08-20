import subprocess
import time
import yaml
import paramiko
import os
import stat
from concurrent.futures import ThreadPoolExecutor

import sys
# 获取当前脚本所在的目录
script_dir = os.path.dirname(os.path.abspath(__file__))
# 获取上级目录
parent_dir = os.path.dirname(script_dir)
# 添加上级目录到系统路径中
sys.path.append(parent_dir)

from file import SSHSingleton, FileTransfer


class Container:
    def __init__(self, local_path=None, remote_path=None, location_type='local',service_name=None, remote_host=None, remote_user=None, private_key_path=None,remote_password=None,
                 max_attempts=10, sleep_time=5):
        self.local_path = local_path
        self.remote_path = remote_path
        self.service_name = service_name
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.remote_password = remote_password
        self.location_type = location_type
        self.max_attempts = max_attempts
        self.sleep_time = sleep_time
        self.private_key_path = private_key_path
        self.load_config()
        self.ssh_singleton = SSHSingleton()
        self.local_yml_path = self.get_local_yml_path()
        self.remote_yml_path = self.get_remote_yml_path()


    def _establish_ssh_connection(self):
        self.ssh_singleton.connect(self.remote_host, self.remote_user, password=self.remote_password, private_key_path=self.private_key_path)

    def execute_ssh_command(self, command):
        print("Executing SSH command:", command)
        self._establish_ssh_connection()
        ssh = self.ssh_singleton.get_ssh()
        stdin, stdout, stderr = ssh.exec_command(command)
        stdin = None  # 不需要处理标准输入，将其置为 None      
        output = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')        
        return stdin,output, error
        
    # local_path目录里可能不仅有yaml文件，还有其他文件，所以要找到yaml文件
    def get_local_yml_path(self):
        for root, dirs, files in os.walk(self.local_path):
            for file in files:
                if file == "docker-compose.yml":
                    return os.path.join(root, file)
        raise FileNotFoundError("docker-compose.yml not found in the specified directory.")
       
  
    def get_remote_yml_path(self):
        command = f"find {self.remote_path} -type f -name docker-compose.yml"
        _,stdout, stderr = self.execute_ssh_command(command)
        remote_yml_path = stdout.strip()
    
        # 如果远程路径中没有找到yml文件，那么上传文件
        if not remote_yml_path:
            transfer = FileTransfer(self.remote_host, self.remote_user, self.remote_password,self.private_key_path)    
            transfer.upload(self.local_path, self.remote_path)
    
            # 再次尝试获取远程路径中的yml文件
            _,stdout, stderr = self.execute_ssh_command(command)
            remote_yml_path = stdout.strip()
    
            # 如果还是没有找到yml文件，那么抛出异常
            if not remote_yml_path:
                raise Exception("Failed to find yml file in remote path after uploading.")
    
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
        command = f"cd {os.path.dirname(self.remote_yml_path)} && sudo docker-compose -f {self.remote_yml_path} up -d {name}"
        self.execute_ssh_command(command)

        if name is not None and not self.wait_for_container_ready(name):
            print(f"Failed to start {name} service: service is not ready.")
            return

    def up_service_local(self, name):
        print(f"Starting local {name} service...")
        subprocess.run(["sudo", "docker-compose", "-f", self.local_yml_path, "up", "-d", name],
                       cwd=os.path.dirname(self.local_yml_path))

        if name is not None and not self.wait_for_container_ready(name):
            print(f"Failed to start local {name} service: service is not ready.")
            return

    def up_all_services(self):
        # 启动所有服务
        if self.location_type == 'remote':
            print("Starting all services on remote host...")
            command = f"cd {self.remote_path} && sudo docker-compose -f {os.path.basename(self.local_yml_path)} up -d"
            self.execute_ssh_command(command)
        elif self.location_type == 'local':
           print("Starting all local vervices...")
           subprocess.run(["sudo", "docker-compose", "-f", self.local_yml_path, "up", "-d"],
                       cwd=os.path.dirname(self.local_yml_path))
    
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")

    def wait_for_container_ready(self, name):
        print("Waiting for service to be ready...")
        print(self.max_attempts)
        for attempt in range(1, self.max_attempts + 1):
            print(f"Attempt {attempt}/{self.max_attempts}")
            time.sleep(self.sleep_time)
            if self.check_status(name, 'up'):
                print("service is ready.")
                return True
            else:
                print("container is not ready.")
        print("service is not ready after {} attempts.".format(self.max_attempts))
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
            _,stdout, stderr = self.execute_ssh_command(" ".join(command))
            if name in stdout:
                return True
        elif self.location_type == 'local':
            result = subprocess.run(command, capture_output=True, text=True)
            if result.returncode == 0 and name in result.stdout:
                return True
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")
        return False

    
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
        command = f"cd {os.path.dirname(self.remote_yml_path)} && sudo docker-compose -f {self.remote_yml_path} stop {name}"
        self.execute_ssh_command(command)

        if name is not None and not self.wait_for_container_stopped(name):
            print(f"Failed to stop {name} service.")

    def stop_service_local(self, name):
        print(f"Stopping local {name} service...")
        subprocess.run(["sudo", "docker-compose", "-f", self.local_yml_path, "stop", name],
                       cwd=os.path.dirname(self.local_yml_path))

        if name is not None and not self.wait_for_container_stopped(name):
            print(f"Failed to stop local {name} service.")

    def stop_all_services(self):
        # 停止所有服务
        if self.location_type == 'remote':
            print("Stopping all service on remote host...")
            command = f"cd {self.remote_path} && sudo docker-compose -f {os.path.basename(self.local_yml_path)} stop"
            self.execute_ssh_command(command)
        elif self.location_type == 'local':
            print("Stopping all local service...")
            subprocess.run(["sudo", "docker-compose", "-f", self.local_yml_path, "stop"],
                           cwd=os.path.dirname(self.local_yml_path))
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")

    def wait_for_container_stopped(self, name):
        print("Waiting for service to be stopped...")
        for attempt in range(1, self.max_attempts + 1):
            print(f"Attempt {attempt}/{self.max_attempts}")
            time.sleep(self.sleep_time)
            if not self.check_status(name,'stopped'):
                print("service is stopped.")
                return True
            else:
                print("service is still running.")
        print("Database is still running after {} attempts.".format(self.max_attempts))
        return False

    def down_services(self, remove_volumes=False):
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

    def down_service(self, name, remove_volumes=False):
        if self.location_type == 'remote':
            self.down_service_remote(name, remove_volumes)
        elif self.location_type == 'local':
            self.down_service_local(name, remove_volumes)
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")

    def get_volumes(self, service_name):
        volumes = []
        yml_path = self.local_yml_path
        directory_name = os.path.basename(os.path.dirname(yml_path))
        if service_name in self.config['services'] and 'volumes' in self.config['services'][service_name]:
            for volume in self.config['services'][service_name]['volumes']:
                volumes.append(f"{directory_name}_{volume.split(':')[0]}")
        return volumes

    def down_service_remote(self, name, remove_volumes=False):
        print(f"Removing {name} service ...")
        # Remove the service containers
        command = (f"cd {os.path.dirname(self.remote_yml_path)} && "
                   f"sudo docker-compose -f {self.remote_yml_path} down --remove-orphans {name}")
        self.execute_ssh_command(command)
        
        if remove_volumes:
            # Remove the volumes associated with the service
            print(f"Removing volumes associated with {name} service...")
            volumes = self.get_volumes(name)
            for volume in volumes:
                command = (f"cd {os.path.dirname(self.remote_yml_path)} && "f"sudo docker volume rm {volume}")
                self.execute_ssh_command(command)

        if name is not None and not self.wait_for_container_removed(name):
            print(f"{name} services may not have been completely removed.")

    def down_service_local(self, name, remove_volumes=False):
        print(f"Removing local {name}  services...")
        # Remove the service containers
        command = ["sudo", "docker-compose", "-f", self.local_yml_path, "down", "--remove-orphans", name]
        subprocess.run(command, cwd=os.path.dirname(self.local_yml_path))
        
        if remove_volumes:
            # Remove the volumes associated with the service
            print(f"Removing all volumes associated with {name} service...")
            volumes = self.get_volumes(name)
            for volume in volumes:
                command = ["sudo", "docker", "volume", "rm", volume]
                subprocess.run(command)

        if not self.wait_for_container_removed(name):
            print(f"Local {name} services may not have been completely removed.")

    def down_all_services(self, remove_volumes=False):
        if self.location_type == 'remote':
            print("Removing all services on remote host...")
            command = f"cd {self.remote_path} && sudo docker-compose -f {os.path.basename(self.remote_yml_path)} down --remove-orphans"
            self.execute_ssh_command(command)
            if remove_volumes:
                # Remove all volumes 
                print(f"Removing all volumes ...")
                command = "sudo docker volume prune --force"
                self.execute_ssh_command(command)
        elif self.location_type == 'local':
            print("Removing all local containers...")
            command = ["sudo", "docker-compose", "-f", self.local_yml_path, "down", "--remove-orphans"]
            subprocess.run(command, cwd=os.path.dirname(self.local_yml_path))
            if remove_volumes:
                # Remove all volumes 
                print(f"Removing all volumes ...")
                command = "sudo docker volume prune --force"
                subprocess.run(command, shell=True)
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")

    def wait_for_container_removed(self, name):
        print("Waiting for service to be removed...")
        for attempt in range(1, self.max_attempts + 1):
            print(f"Attempt {attempt}/{self.max_attempts}")
            time.sleep(self.sleep_time)
            if not self.check_status(name, 'removed'):
                print("service is removed.")
                return True
            else:
                print("container is still running.")
        print("service is not ready after {} attempts.".format(self.max_attempts))
        return False
    
    def backup_service_data_volumes(self, target_directory):
        # 备份指定的服务或所有服务
        if self.service_name is None:
            self.backup_all_service_data_volumes(target_directory)
        elif isinstance(self.service_name, str):
            self.backup_service_data_volume(self.service_name, target_directory)
        elif isinstance(self.service_name, list):
            if self.service_name:
                for name in self.service_name:
                    self.backup_service_data_volume(name, target_directory)
            else:
                self.backup_all_service_data_volumes(target_directory)
        else:
            raise ValueError("Invalid service_name type. Must be a string or a list of strings or None.")

    def backup_service_data_volume(self, name, target_directory):
        if self.location_type == 'remote':
            self.backup_service_data_volume_remote(name, target_directory)
        elif self.location_type == 'local':
            self.backup_service_data_volume_local(name, target_directory)
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")

        
    def backup_service_data_volume_remote(self, name, target_directory):
        print(f"Backing up {name} service data volumes on remote host...")
        volumes = self.get_volumes(name)
        for volume in volumes:
            backup_script = f"""
                #!/bin/bash
                docker run --rm -v {volume}:/volume_data -v {target_directory}:/backup/{name} busybox cp -r /volume_data /backup/{name}
            """
            command = f"echo '{backup_script}' > /tmp/service_backup.sh && chmod +x /tmp/service_backup.sh && /tmp/service_backup.sh"
            self.execute_ssh_command(command)

    def backup_service_data_volume_local(self, name, target_directory):
        print(f"Backing up {name} service data volumes locally...")
        volumes = self.get_volumes(name)
        for volume in volumes:
            backup_script = f"""
                #!/bin/bash
                docker run --rm -v {volume}:/volume_data -v {target_directory}:/backup/{name} busybox cp -r /volume_data /backup/{name}
            """
            with open("/tmp/service_backup.sh", "w") as f:
                f.write(backup_script)
            subprocess.run(["bash", "/tmp/service_backup.sh"])
            os.remove("/tmp/service_backup.sh")


    def backup_all_service_data_volumes(self, target_directory):
        # 备份所有数据卷
        if self.location_type == 'remote':
            yml_path = self.remote_yml_path
        elif self.location_type == 'local':
            yml_path = self.local_yml_path
        else:
            raise ValueError("Invalid location_type. Must be 'remote' or 'local'.")

        directory_name = os.path.basename(os.path.dirname(yml_path))
        for service_name in self.config['services']:
            if 'volumes' in self.config['services'][service_name]:
                volumes = self.get_volumes(service_name)
                for volume in volumes:
                    if self.location_type == 'remote':
                        self.backup_service_data_volume_remote(volume, target_directory, directory_name, service_name)
                    elif self.location_type == 'local':
                        self.backup_service_data_volume_local(volume, target_directory, directory_name, service_name)

"""
attention:
1、当程序调试时，如果出现问题，可以手动执行命令，查看具体错误信息，比如，有次因为我sudoers文件配置错误，导致无法执行sudo命令，所以程序执行失败。
但是，程序中并没有显示这个错误信息，所以，可以手动执行命令，查看具体错误信息。
2、在执行命令时，本程序中的命令是通过paramiko模块执行的，所以，如果执行的命令中有sudo命令，需要注意，paramiko执行的命令是没有sudo权限的，
所以，如果执行的命令中有sudo命令，需要在命令前加sudo，或者在命令前加sudo -S，然后在命令后加上密码，这样就可以执行sudo命令了。
不过，最好在sudoers文件中配置免密码执行sudo命令。
3、本程序中需要执行docker命令，所以，一定保证执行paramiko的相关命令时要CD到docker-compose.yml文件所在的目录，否则，docker-compose命令无法执行。
其次，本程序定义了需查找docker-compose.yml文件所在的目录的方法，但是，有可能找到多个yml文件，这样，要保证。只能找到一个yml文件，否则，会出错。当然，也不能没有yml文件。
所以，可以保证上传的文件夹中的只有一个yml文件名
4.低版本的parakimo不能正常运行，版本3.4经测试可以正常运行
"""

if __name__ == "__main__":  
      
    #定义要创建的容器是本地还是远程    
    location_type = 'remote'
    
    #定义多个服务名称
    # service_names_batch = ["p0_s_mysql_master_1"]
    # service_names_batch = ["p0_s_redis_master_1"]
    service_names_batch = ["p0_s_mysql_master_1","p0_s_redis_master_1","p0_s_tracker_master_1","p0_s_storage_master_1"]
    
    #定义创建容器的yaml文件及其相关文件的地址
    local_path = '/home/zym/container/'
    #远程路径中可以有yaml文件，也可以没有，如果没有，那么，它必须是个文件夹
    remote_path = '/home/zym/'
    
    # 定义远程连接时所需要的主机地址，用户名和密钥或密码
    remote_host = '47.103.135.26'   
    remote_user = 'zym'
    private_key_path = '/home/zym/.ssh/new_key'
    remote_password = "alyfwqok"
    #定义创建远程容器的尝试次数和时间
    max_attempts = 10
    sleep_time = 5
    
     #创建远程实例并启动批量服务 用ssh密钥连接
    db_batch = Container(location_type=location_type, 
                                service_name=service_names_batch,
                                local_path=local_path, remote_path=remote_path, remote_host=remote_host, remote_user=remote_user,
                                private_key_path=private_key_path,remote_password=remote_password)
    
    # #创建实例并启动批量服务  用ssh密钥连接   
    # db_batch.up_services()
    
    # #创建实例并停止批量服务 用ssh密钥连接
    # db_batch.stop_services()
    
    #创建远程实例并移除批量服务(而且移除相关数据卷) 用ssh密钥连接
    db_batch.down_services(remove_volumes=True)
    
    
    
    #定义要创建的容器是本地还是远程    
    location_type = 'local'
    
    #定义多个服务名称
    # service_names_batch = ["p0_s_mysql_slave_1"]
    # service_names_batch = ["p0_s_redis_slave_1"]
    service_names_batch = ["p0_s_mysql_slave_1","p0_s_redis_slave_1","p0_s_tracker_slave_1","p0_s_storage_slave_1"]
    
    #定义创建容器的yaml文件及其相关文件的地址
    local_path = '/home/zym/container/'
    #远程路径中可以有yaml文件，也可以没有，如果没有，那么，它必须是个文件夹
    remote_path = '/home/zym/'
    
    # 定义远程连接时所需要的主机地址，用户名和密钥或密码
    remote_host = '47.103.135.26'   
    remote_user = 'zym'
    private_key_path = '/home/zym/.ssh/new_key'
    remote_password = "alyfwqok"
    #定义创建远程容器的尝试次数和时间
    max_attempts = 10
    sleep_time = 5
    
    #创建远程实例并启动批量服务 用ssh密钥连接
    db_batch = Container(location_type=location_type, 
                                service_name=service_names_batch,
                                local_path=local_path, remote_path=remote_path, remote_host=remote_host, remote_user=remote_user,
                                private_key_path=private_key_path,remote_password=remote_password)
    
    # #创建实例并启动批量服务  用ssh密钥连接   
    # db_batch.up_services()
    
    # #创建实例并停止批量服务 用ssh密钥连接
    # db_batch.stop_services()
    
    #创建远程实例并移除批量服务(而且移除相关数据卷) 用ssh密钥连接
    db_batch.down_services(remove_volumes=True)
    
    
     #定义要创建的容器是本地还是远程    
    location_type = 'remote'
    
    #定义多个服务名称
    # service_names_batch = ["p0_s_mysql_master_1"]
    # service_names_batch = ["p0_s_redis_master_1"]
    service_names_batch = ["p0_s_mysql_master_1","p0_s_redis_master_1","p0_s_tracker_master_1","p0_s_storage_master_1"]
    
    #定义创建容器的yaml文件及其相关文件的地址
    local_path = '/home/zym/container/'
    #远程路径中可以有yaml文件，也可以没有，如果没有，那么，它必须是个文件夹
    remote_path = '/home/zym/'
    
    # 定义远程连接时所需要的主机地址，用户名和密钥或密码
    remote_host = '47.103.135.26'   
    remote_user = 'zym'
    private_key_path = '/home/zym/.ssh/new_key'
    remote_password = "alyfwqok"
    #定义创建远程容器的尝试次数和时间
    max_attempts = 10
    sleep_time = 5
    
     #创建远程实例并启动批量服务 用ssh密钥连接
    db_batch = Container(location_type=location_type, 
                                service_name=service_names_batch,
                                local_path=local_path, remote_path=remote_path, remote_host=remote_host, remote_user=remote_user,
                                private_key_path=private_key_path,remote_password=remote_password)
    
    # #创建实例并启动批量服务  用ssh密钥连接   
    db_batch.up_services()
    
    # #创建实例并停止批量服务 用ssh密钥连接
    # db_batch.stop_services()
    
    #创建远程实例并移除批量服务(而且移除相关数据卷) 用ssh密钥连接
    # db_batch.down_services(remove_volumes=True)
    
    
    
    #定义要创建的容器是本地还是远程    
    location_type = 'local'
    
    #定义多个服务名称
    # service_names_batch = ["p0_s_mysql_slave_1"]
    # service_names_batch = ["p0_s_redis_slave_1"]
    service_names_batch = ["p0_s_mysql_slave_1","p0_s_redis_slave_1","p0_s_tracker_slave_1","p0_s_storage_slave_1"]
    
    #定义创建容器的yaml文件及其相关文件的地址
    local_path = '/home/zym/container/'
    #远程路径中可以有yaml文件，也可以没有，如果没有，那么，它必须是个文件夹
    remote_path = '/home/zym/'
    
    # 定义远程连接时所需要的主机地址，用户名和密钥或密码
    remote_host = '47.103.135.26'   
    remote_user = 'zym'
    private_key_path = '/home/zym/.ssh/new_key'
    remote_password = "alyfwqok"
    #定义创建远程容器的尝试次数和时间
    max_attempts = 10
    sleep_time = 5
    
    #创建远程实例并启动批量服务 用ssh密钥连接
    db_batch = Container(location_type=location_type, 
                                service_name=service_names_batch,
                                local_path=local_path, remote_path=remote_path, remote_host=remote_host, remote_user=remote_user,
                                private_key_path=private_key_path,remote_password=remote_password)
    
    # #创建实例并启动批量服务  用ssh密钥连接   
    db_batch.up_services()
    
    # #创建实例并停止批量服务 用ssh密钥连接
    # db_batch.stop_services()
    
    #创建远程实例并移除批量服务(而且移除相关数据卷) 用ssh密钥连接
    # db_batch.down_services(remove_volumes=True)
    
    
    
    # # 指定本地备份目录
    # backup_directory = '/path/to/backup/directory'
    
    # # 创建远程实例并备份单个服务
    # db_remote_single = Container(local_path=local_path, remote_path=remote_path,service_name=service_name_single,
    #                                   remote_host=remote_host, remote_user=remote_user, remote_password=remote_password,
    #                                   location_type='remote')
    # db_remote_single.backup_service_data_volumes(backup_directory)    
    
    
    
    
