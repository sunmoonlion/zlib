import os
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

    def create_remote_directory(self, remote_path):
        self._establish_ssh_connection()
        ssh = self.ssh_singleton.get_ssh()
        stdin, stdout, stderr = ssh.exec_command(f"mkdir -p {remote_path}")
        if stderr.read().strip():
            raise RuntimeError(f"Failed to create remote directory {remote_path}")

    def upload(self, local_path, remote_path):
        if os.path.isdir(local_path):
            # 首先处理本地路径，并将本地路径规范化（路径为目录则以"/"结束）
            # 如果是目录，检查路径是否以斜杠结尾
            if not local_path.endswith('/'):
                # 如果不是，添加斜杠，从而规范化
                local_path += '/' 
            #处理本地上传目录的结构，便于在远程创建相同的目录结构           
            local_dirs = self._get_directory_structure(local_path)
            #接着处理远程路径，首先规范路径，统一以'/'结束
            if not remote_path.endswith('/'):
                # 如果不是，增加斜杠
                remote_path += '/'
            #当本地上传的是目录时，我们将该目录整体上传，因此，传入的远程目录路径要和本地上传的目录名作拼接
            remote_folder = os.path.join(remote_path,os.path.basename(local_path[:-1]))
            
            #创建远程目录，上传文件            
            self._create_remote_directory_structure(local_dirs, remote_folder)
            self._upload_directory(local_path, remote_folder)
        else:
            # 首先处理本地路径，并将本地路径规范化（路径包含文件，不以"/"结束）
            #如果是文件，错误传入'/',则移除斜杠
            if local_path.endswith('/'):
                # 如果是，移除斜杠
                local_path = local_path[:-1]
                print(local_path)
            #接着处理远程路径，首先规范路径，统一以'/'结束
            if not remote_path.endswith('/'):
                # 如果不是，增加斜杠
                remote_path += '/'         
            
            self.create_remote_directory(remote_path)
            
            remote_path = os.path.join(remote_path, os.path.basename(local_path))
            self._upload_file(local_path, remote_path)

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

    def close_ssh_connection(self):
        self.ssh_singleton.close()

if __name__ == "__main__":
    # 使用示例
    remote_host = '47.103.135.26'
    remote_user = 'zym'
    remote_password = 'alyfwqok'  # 这应该用更安全的认证方法替换
    file_transfer = FileTransfer(remote_host, remote_user, remote_password)

    # 上传文件或文件夹
    local_item = '/home/WUYING_13701819268_15611880/Desktop/kk'  # 可以是文件或文件夹
    remote_item = '/home/zym/container/docker/mm'  # 远程位置
    try:
        file_transfer.upload(local_item, remote_item)
        print("上传成功。")
    except RuntimeError as e:
        print(f"上传失败：{str(e)}")
    finally:
        file_transfer.close_ssh_connection()
