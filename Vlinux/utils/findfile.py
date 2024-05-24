import os
import subprocess
import platform
import paramiko

def get_local_file_path(filename):
    try:
        # 获取当前操作系统类型
        system = platform.system()
        
        if system == "Windows":
            # 本地 Windows 系统
            command = ["where", filename]
        elif system in ["Linux", "Darwin"]:
            # 本地 Linux 或 macOS 系统
            command = ["which", filename]
        else:
            # 其他系统暂不支持
            raise RuntimeError("Unsupported operating system")

        # 执行命令，获取输出
        result = subprocess.run(command, capture_output=True, text=True)
        
        # 如果命令成功执行，返回输出结果
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            # 如果文件不在 PATH 中，则在全文件系统中查找
            if system == "Windows":
                search_command = ["dir", f"/s /b {filename}"]
            else:
                search_command = ["find", "/", "-name", filename]

            search_result = subprocess.run(search_command, capture_output=True, text=True)

            if search_result.returncode == 0 and search_result.stdout.strip():
                return f"{search_result.stdout.strip()} (not in PATH)"
            else:
                return None
    except Exception as e:
        print(f"An error occurred while finding local {filename} path: {e}")
        return None

def get_remote_file_path(host, username, password, filename):
    try:
        # 远程 Linux 系统
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, username=username, password=password)
        
        # 获取远程环境变量
        stdin, stdout, stderr = ssh.exec_command("echo $PATH")
        remote_path = stdout.read().decode().strip()
        print("Remote PATH:", remote_path)
        
        # 查找文件路径
        command = f"which {filename}"
        stdin, stdout, stderr = ssh.exec_command(command)
        remote_file_path = stdout.read().decode().strip()
        
        if not remote_file_path:
            # 如果文件不在 PATH 中，则在全文件系统中查找
            find_command = f"find / -name {filename} 2>/dev/null"
            stdin, stdout, stderr = ssh.exec_command(find_command)
            remote_file_path = stdout.read().decode().strip()

            if remote_file_path:
                return f"{remote_file_path} (not in PATH)"
            else:
                return None
        
        ssh.close()
        
        return remote_file_path
    except Exception as e:
        print(f"An error occurred while finding remote {filename} path: {e}")
        return None

if __name__=="__main__":
        

    # 示例用法
    local_file_path = get_local_file_path('mysqldump')
    if local_file_path:
        print(f"Local mysqldump path: {local_file_path}")
    else:
        print("Failed to find local mysqldump path")

    # 示例用法
    remote_file_path = get_remote_file_path('your_remote_host', 'your_username', 'your_password', 'mysqldump')
    if remote_file_path:
        print(f"Remote mysqldump path: {remote_file_path}")
    else:
        print("Failed to find remote mysqldump path")
