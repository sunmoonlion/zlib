import paramiko

class FileTransfer:
    def __init__(self, remote_host, remote_user, remote_password):
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.remote_password = remote_password

    def upload_file(self, local_path, remote_path):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.remote_host, username=self.remote_user, password=self.remote_password)
        
        sftp = ssh.open_sftp()
        sftp.put(local_path, remote_path)
        sftp.close()
        ssh.close()

    def download_file(self, remote_path, local_path):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.remote_host, username=self.remote_user, password=self.remote_password)
        
        sftp = ssh.open_sftp()
        sftp.get(remote_path, local_path)
        sftp.close()
        ssh.close()

# 示例用法
if __name__ == "__main__":
    remote_host = "your_remote_host"
    remote_user = "your_remote_user"
    remote_password = "your_remote_password"

    file_transfer = FileTransfer(remote_host, remote_user, remote_password)
    file_transfer.upload_file("docker-compose.yml", "/path/to/docker-compose.yml")
