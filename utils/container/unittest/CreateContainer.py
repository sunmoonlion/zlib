import subprocess
import time
import yaml
import paramiko
import os


class CreateContainer:
    def __init__(self, local_yml_path, service_name=None, remote_host=None, remote_user=None, remote_password=None,
                 db_type='local', remote_yml_dir=None, max_attempts=10, sleep_time=5):
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.remote_password = remote_password
        self.db_type = db_type
        self.local_yml_path = local_yml_path
        self.remote_yml_dir = remote_yml_dir
        self.service_name = service_name
        self.max_attempts = max_attempts
        self.sleep_time = sleep_time
        self.load_config()

    def load_config(self):
        with open(self.local_yml_path, "r") as config_file:
            self.config = yaml.safe_load(config_file)

    def start_databases(self):
        if self.service_name is None:
            self.start_all_databases()
        elif isinstance(self.service_name, str):
            self.start_database(self.service_name)
        elif isinstance(self.service_name, list):
            if self.service_name:
                for name in self.service_name:
                    self.start_database(name)
            else:
                self.start_all_databases()
        else:
            raise ValueError("Invalid service_name type. Must be a string or a list of strings or None.")

    def start_database(self, name):
        if self.db_type == 'remote':
            self.start_database_remote(name)
        elif self.db_type == 'local':
            self.start_database_local(name)
        else:
            raise ValueError("Invalid db_type. Must be 'remote' or 'local'.")

    def start_database_remote(self, name):
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

    def start_database_local(self, name):
        print(f"Starting local {name} database...")
        subprocess.run(["sudo", "docker-compose", "-f", self.local_yml_path, "up", "-d", name],
                       cwd=os.path.dirname(self.local_yml_path))

        if name is not None and not self.wait_for_db_ready(name):
            print(f"Failed to start local {name} database: Database is not ready.")
            return

    def start_all_databases(self):
        if self.db_type == 'remote':
            print("Starting all databases on remote host...")
            command = f"cd {self.remote_yml_dir} && sudo docker-compose -f {os.path.basename(self.local_yml_path)} up -d"
            self.execute_ssh_command(command)
        elif self.db_type == 'local':
            print("Starting all local databases...")
            subprocess.run(["sudo", "docker-compose", "-f", self.local_yml_path, "up", "-d"],
                           cwd=os.path.dirname(self.local_yml_path))
        else:
            raise ValueError("Invalid db_type. Must be 'remote' or 'local'.")

    def wait_for_db_ready(self, name):
        print("Waiting for database to be ready...")
        for attempt in range(1, self.max_attempts + 1):
            print(f"Attempt {attempt}/{self.max_attempts}")
            time.sleep(self.sleep_time)
            if self.check_database(name):
                print("Database is ready.")
                return True
            else:
                print("Database is not ready.")
        print("Database is not ready after {} attempts.".format(self.max_attempts))
        return False

    def check_database(self, name):
        container_name = self.config['services'][name]['container_name']
        if self.db_type == 'remote':
            stdout, stderr = self.execute_ssh_command(f"sudo docker ps | grep {container_name} | awk '{{print $NF}}'")
            if container_name in stdout:
                return True
        elif self.db_type == 'local':
            result = subprocess.run(
                ["sudo", "docker", "ps", "--filter", f"name={container_name}", "--format", "{{.Names}}"],
                capture_output=True, text=True)
            if result.returncode == 0 and container_name in result.stdout:
                return True
        else:
            raise ValueError("Invalid db_type. Must be 'remote' or 'local'.")
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
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.remote_host, username=self.remote_user, password=self.remote_password)
        sftp = ssh.open_sftp()
        sftp.put(local_path, remote_path)
        sftp.close()
        ssh.close()


if __name__ == "__main__":
    remote_host = "47.103.135.26"
    remote_user = "zym"
    remote_password = "alyfwqok"

    local_yml_path = "/home/WUYING_13701819268_15611880/Desktop/docker-compose.yml"

    remote_yml_dir = "/home/zym/test1"

    service_name_single_local = "service1"
    db_local_single = CreateContainer(local_yml_path=local_yml_path, service_name=service_name_single_local)
    db_local_single.start_databases()

    db_remote_single = CreateContainer(local_yml_path=local_yml_path, service_name=service_name_single_local,
                                        remote_host=remote_host, remote_user=remote_user, remote_password=remote_password,
                                        db_type='remote', remote_yml_dir=remote_yml_dir)
    db_remote_single.start_databases()

    service_names_batch_local = ["service1", "service2", "service3"]
    db_local_batch = CreateContainer(local_yml_path=local_yml_path, service_name=service_names_batch_local)
    db_local_batch.start_databases()

    db_remote_batch = CreateContainer(local_yml_path=local_yml_path, service_name=service_names_batch_local,
                                       remote_host=remote_host, remote_user=remote_user, remote_password=remote_password,
                                       db_type='remote', remote_yml_dir=remote_yml_dir)
    db_remote_batch.start_databases()

    db_local_all = CreateContainer(local_yml_path=local_yml_path)
    db_local_all.start_databases()

    db_remote_all = CreateContainer(local_yml_path=local_yml_path, remote_host=remote_host, remote_user=remote_user,
                                     remote_password=remote_password, db_type='remote', remote_yml_dir=remote_yml_dir)
    db_remote_all.start_databases()
