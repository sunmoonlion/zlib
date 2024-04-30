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
                local_dirs = self._get_directory_structure(local_path)  # 获取目录结构
                remote_folder = os.path.join(remote_path, os.path.basename(local_path.rstrip('/')))
                self._create_remote_directory_structure(local_dirs, remote_folder)
                self._upload_directory(local_path, remote_folder)
            else:
                remote_file_path = os.path.join(remote_path, os.path.basename(local_path))
                self.create_remote_directory(remote_path)
                self._upload_file(local_path, remote_file_path)
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

            # 确保本地目录存在
            os.makedirs(local_path, exist_ok=True)

            try:
                if self._is_remote_directory(sftp, remote_path):
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
        os.makedirs(local_dir, exist_ok=True)  # 创建本地目录
        files_and_directories = sftp.listdir_attr(remote_path)
        for item in files_and_directories:
            item_name = item.filename
            remote_item_path = os.path.join(remote_path, item_name)
            local_item_path = os.path.join(local_dir, item_name)
            if stat.S_ISDIR(item.st_mode):
                self._download_directory(sftp, remote_item_path, local_item_path)
            else:
                self._download_file(sftp, remote_item_path, local_dir)


if __name__ == "__main__":
    # 使用示例
    remote_host = '47.103.135.26'
    remote_user = 'zym'
    remote_password = 'alyfwqok'  # 这应该用更安全的认证方法替换
    file_transfer = FileTransfer(remote_host, remote_user, remote_password)

    # 文件夹
    local_item = '/home/WUYING_13701819268_15611880/Desktop/zlib/'  # 可以是文件或文件夹
    remote_item = '/home/zym/container/'  # 远程位置
    file_transfer.upload(local_item, remote_item)

    # # 上传文件
    # local_item = '/home/WUYING_13701819268_15611880/Desktop/docker-compose.yml'  # 可以是文件或文件夹
    # remote_item = '/home/zym/container/'  # 远程位置
    # file_transfer.upload(local_item, remote_item)

    # # 下载文件夹
    # local_item = '/home/WUYING_13701819268_15611880/Desktop/hehe'  # 可以是文件或文件夹
    # remote_item = '/home/zym/container/'  # 远程位置
    # file_transfer.download(remote_item, local_item)

    # # 下载文件
    # local_item = '/home/WUYING_13701819268_15611880/Desktop/kk/'  # 可以是文件或文件夹
    # remote_item = '/home/zym/container/docker-compose.yml'  # 远程位置
    # file_transfer.download(remote_item, local_item)
