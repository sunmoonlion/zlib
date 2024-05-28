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

    def connect(self, host, username, password=None, private_key_path=None):
        if password is None and private_key_path is None:
            raise ValueError("Either password or private_key_path must be provided.")
        
        if self._ssh is None or not self._ssh.get_transport().is_active():
            try:
                self._ssh = paramiko.SSHClient()
                self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                print(f"Trying to connect to {host}...")
                if private_key_path:
                    private_key = paramiko.RSAKey.from_private_key_file(private_key_path)
                    self._ssh.connect(host, username=username, pkey=private_key)                    
                else:
                    self._ssh.connect(host, username=username, password=password)
                print(f"Connected to {host}.")
            except paramiko.AuthenticationException as e:
                print(f"Failed to authenticate with host {host}: {str(e)}")
                raise
            except paramiko.SSHException as e:
                print(f"SSH connection to host {host} failed: {str(e)}")
                raise
            except Exception as e:
                print(f"An unexpected error occurred: {str(e)}")
                raise
    def get_ssh(self):
        return self._ssh

    def close(self):
        if self._ssh is not None:
            self._ssh.close()
            self._ssh = None

class FileTransfer: 
    def __init__(self, remote_host, remote_user, remote_password=None, private_key_path=None):
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.remote_password = remote_password
        self.private_key_path = private_key_path
        self.ssh_singleton = SSHSingleton()

    def _establish_ssh_connection(self):
        self.ssh_singleton.connect(self.remote_host, self.remote_user, password=self.remote_password, private_key_path=self.private_key_path)

    def _normalize_path(self, path, is_directory):
        # 修正路径格式,因为有时路径中有不规则的符号，这是os自带的方法
        normalized_path = os.path.normpath(path)
        #os自带的方法不能处理用户不规范输入的路径，所以这里手动处理（比如，用户输入的路径最后没有/，但是是文件夹），以便后续处理时能正确判断并准确提取文件名或文件夹名
        if is_directory:
            if not normalized_path.endswith('/'):
                normalized_path += '/'
        else:
            #os自带的方法不能处理用户不规范输入的路径，所以这里手动处理（比如，用户输入的路径带有/，但是是文件，所以要去掉）
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
        #create_remote_directory只是创建了一个目录，这里要递归创建所有目录
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
        print('start uploading...')
        try:
            if os.path.isdir(local_path):
                #os.path.isdir是判断是否是文件夹，不是文件，它只是判断最后不是/的部分，无论是否是/结尾，都会返回True，比如/home/zym/和/home/zym是一样的
                #而os.path.dir()是判断最后一个/后面的部分，所以要把路径规范化，以正确提取文件名或文件夹名
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
                
            print("uploadling success!")  
        except RuntimeError as e:
            print(f"uploading failure：{str(e)}")
            raise e
        finally:
            self.ssh_singleton.close()

    def download(self, remote_path, local_path):
        print('start downloading... ')
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
                print("download success!")
            finally:
                sftp.close()
        except RuntimeError as e:
            print(f"download failure：{str(e)}")
            raise e
        finally:
            self.ssh_singleton.close()

    def _download_file(self, sftp, remote_path, local_dir):
        remote_filename = os.path.basename(remote_path)
        local_path = os.path.join(local_dir, remote_filename)
        sftp.get(remote_path, local_path)

    def _download_directory(self, sftp, remote_path, local_dir):
        #注意，upload和download的逻辑不一样，upload是先创建所有的远程文件夹，再把上传文件到合适的文件夹，而download是每一层文件夹一层一层递归处理，即递归创建每层文件夹
        # 创建文件夹后，随即下载文件
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


if __name__ == "__main__":
    # 使用示例
    remote_host = '47.103.135.26'
    remote_user = 'zym'
    
    #定义本地主机的私钥路径
    private_key_path = "/home/zym/.ssh/new_key"
    
    # # #定义远程主机的密码,如果使用私钥连接则不需要!!!!
    remote_password = "alyfwqok"
    # # 上传要求传入一个文件夹！！！！！
    transfer = FileTransfer(remote_host, remote_user, private_key_path=private_key_path)
    
    # transfer = FileTransfer(remote_host, remote_user, remote_password=remote_password)
   
    # # 文件夹
    # local_item = '/home/zym/zlib/'  # 可以是文件或文件夹
    # remote_item = '/home/zym/container/'  # 远程位置
    
    # transfer.upload(local_item, remote_item)

    # 上传文件
    local_item = '/home/zym/web_meiduo_mall_docker/backend/mysql/master_db2.sql'  # 可以是文件或文件夹
    remote_item = '/home/zym/container/'  # 远程位置
    transfer.upload(local_item, remote_item)

    # # 下载文件夹
    # local_item = '/home/WUYING_13701819268_15611880/Desktop/hehe'  # 可以是文件或文件夹
    # remote_item = '/home/zym/container/'  # 远程位置
    # transfer.download(remote_item, local_item)

    # # 下载文件
    # local_item = '/home/WUYING_13701819268_15611880/Desktop/kk/'  # 可以是文件或文件夹
    # remote_item = '/home/zym/container/docker-compose.yml'  # 远程位置
    # transfer.download(remote_item, local_item)
