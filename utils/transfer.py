import os
import paramiko

class FileTransfer:
    def __init__(self, remote_host, remote_user, remote_password):
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.remote_password = remote_password

    def upload(self, local_path, remote_path):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.remote_host, username=self.remote_user, password=self.remote_password)
        
        sftp = ssh.open_sftp()
        if os.path.isfile(local_path):
            sftp.put(local_path, remote_path)
        elif os.path.isdir(local_path):
            self._upload_directory(sftp, local_path, remote_path)
        sftp.close()
        ssh.close()

    def download(self, remote_path, local_path):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.remote_host, username=self.remote_user, password=self.remote_password)
        
        sftp = ssh.open_sftp()
        if self._is_remote_directory(sftp, remote_path):
            self._download_directory(sftp, remote_path, local_path)
        else:
            sftp.get(remote_path, local_path)
        sftp.close()
        ssh.close()

    def _upload_directory(self, sftp, local_path, remote_path):
        for root, dirs, files in os.walk(local_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                remote_file_path = os.path.join(remote_path, os.path.relpath(local_file_path, local_path))
                sftp.put(local_file_path, remote_file_path)

    def _download_directory(self, sftp, remote_path, local_path):
        os.makedirs(local_path, exist_ok=True)
        for item in sftp.listdir_attr(remote_path):
            remote_item_path = os.path.join(remote_path, item.filename)
            local_item_path = os.path.join(local_path, item.filename)
            if stat.S_ISDIR(item.st_mode):
                self._download_directory(sftp, remote_item_path, local_item_path)
            else:
                sftp.get(remote_item_path, local_item_path)

    def _is_remote_directory(self, sftp, remote_path):
        try:
            return stat.S_ISDIR(sftp.stat(remote_path).st_mode)
        except IOError:
            return False