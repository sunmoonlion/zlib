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

    def connect(self, host, username, private_key_path):
        if self._ssh is None or not self._ssh.get_transport().is_active():
            try:
                self._ssh = paramiko.SSHClient()
                self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                # 使用基于密钥的认证
                private_key = paramiko.RSAKey.from_private_key_file(private_key_path)
                self._ssh.connect(host, username=username, pkey=private_key)
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
    def __init__(self, remote_host, remote_user, remote_private_key_path):
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.remote_private_key_path = remote_private_key_path
        self.ssh_singleton = SSHSingleton()

    def _establish_ssh_connection(self):
        self.ssh_singleton.connect(self.remote_host, self.remote_user, self.remote_private_key_path)

    def _normalize_path(self, path, is_directory, remote=False):
        '''这个 _normalize_path 方法用于规范化路径。如果 remote 参数为 True，则表示需要处理远程路径，此时会将 Windows 风格的路径转换为 Unix 风格；否则，根据操作系统规范化本地路径。

            如果 remote 为 True，并且当前操作系统为 Windows，那么将路径中的反斜杠 \ 替换为正斜杠 /，以适应 Unix 风格的路径格式。
            如果 remote 为 False 或者当前操作系统不是 Windows，那么使用 os.path.normpath 方法对路径进行规范化。
            如果 is_directory 参数为 True，则确保路径以文件夹分隔符结尾，否则删除末尾的文件夹分隔符。
            这个方法的作用是确保路径的格式正确，并且根据需要进行格式转换，以便于在不同操作系统和远程服务器上正确处理文件路径。'''
        if remote:
            if os.name == 'nt':
                return path.replace('\\', '/')
            else:
                return path
        else:
            normalized_path = os.path.normpath(path)
            if is_directory:
                if not normalized_path.endswith(os.sep):
                    normalized_path += os.sep
            else:
                if normalized_path.endswith(os.sep):
                    normalized_path = normalized_path[:-1]
            return normalized_path

    def create_remote_directory(self, remote_path):
        self._establish_ssh_connection()
        ssh = self.ssh_singleton.get_ssh()
        stdin, stdout, stderr = ssh.exec_command(f"mkdir -p {self._normalize_path(remote_path, is_directory=True, remote=True)}")
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

    def _upload_file(self, local_file_path, remote_path):
        self._establish_ssh_connection()
        ssh = self.ssh_singleton.get_ssh()
        sftp = ssh.open_sftp()
        try:
            sftp.put(local_file_path, remote_path)
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
                local_path = self._normalize_path(local_path, is_directory=True)
                remote_path = self._normalize_path(remote_path, is_directory=True, remote=True)

                local_dirs = self._get_directory_structure(local_path)
                remote_folder = self._normalize_path(os.path.join(remote_path, os.path.basename(local_path[:-1])), is_directory=True, remote=True)

                self._create_remote_directory_structure(local_dirs, remote_folder)
                self._upload_directory(local_path, remote_folder)
            else:
                local_path = self._normalize_path(local_path, is_directory=False)
                remote_path = self._normalize_path(remote_path, is_directory=False, remote=True)

                self.create_remote_directory(remote_path)
                remote_path = self._normalize_path(os.path.join(remote_path, os.path.basename(local_path)), is_directory=False, remote=True)
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
                    remote_path = self._normalize_path(remote_path, is_directory=True, remote=True)
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


if __name__ == "__main__":
    # 使用示例
    remote_host = '47.103.135.26'
    remote_user = 'root'

    #基于密钥验证
    remote_private_key_path = "C:\\Users\\zym\\.ssh\\aly.pem"
    file_transfer = FileTransfer(remote_host, remote_user, remote_private_key_path)

    # 上传文件或文件夹（问）
    local_item = "C:\\zd_zsone\\etrade.xmb"  # 可以是文件或文件夹
    remote_item = '/home/zym'  # 远程位置
    file_transfer.upload(local_item, remote_item)
