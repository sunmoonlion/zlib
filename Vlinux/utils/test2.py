def get_non_system_databases_remote(self):
        command_show_dbs = (f"{self.remote_mysql_path} -u{self.mysqlusername} -p{self.mysqlpassword} "
                            f"-h{self.mysqlhost} --port={self.mysqlport} -e \"SHOW DATABASES;\"")
        print(f"Executing remote command to fetch databases: {command_show_dbs}")
        
        _, output, error = self.execute_ssh_command(command_show_dbs)
        print(f"Remote command SHOW DATABASES output: {output}")
        print(f"Remote command SHOW DATABASES error output: {error}")

        if error.strip() and 'Using a password on the command line interface can be insecure' not in error:
            raise Exception(f"Error fetching databases: {error}")

        if not output.strip():
            raise Exception("No output received from SHOW DATABASES command")

        databases = output.strip().split('\n')
        print(f"Parsed databases: {databases}")

        non_system_databases = [db for db in databases if db not in ('Database', 'mysql', 'information_schema', 'performance_schema', 'sys')]
        print(f"Non-system databases fetched: {non_system_databases}")
        return non_system_databases

    def export_all_databases_to_sql_file(self, sql_file_path):
        try:
            # 获取非系统数据库列表
            databases = self.get_non_system_databases_remote()
            if not databases:
                print("No databases found or failed to connect to the MySQL server.")
                return

            # 在远程服务器上执行导出命令并将所有数据库写入一个文件
            remote_sql_file_path = os.path.join('/tmp', os.path.basename(sql_file_path))
            database_list = ' '.join(databases)
            remote_command = (f"{self.remote_mysqldump_path} -u{self.mysqlusername} "
                              f"-p{self.mysqlpassword} -h {self.mysqlhost} "
                              f"--port={self.mysqlport} --databases {database_list} > {remote_sql_file_path}")
            print(f"Executing remote command to export databases: {remote_command}")
            _, output, error = self.execute_ssh_command(remote_command)

            print(f"Remote command export output: {output}")
            print(f"Remote command export error output: {error}")

            if error.strip() and 'Using a password on the command line interface can be insecure' not in error:
                raise Exception(f"Error exporting databases remotely: {error}")

            # 下载SQL文件到本地
            transfer = self.get_transfer()
            transfer.download(remote_sql_file_path, "/tmp")

            print(f"All databases exported successfully from remote server.")
        except Exception as e:
            print(f"Error occurred while exporting the database from remote server: {e}")
            raise

    def import_all_databases_from_sql_file(self, sql_file_path):
        try:
            if self.location_type == 'local':
                command = f"{self.local_mysql_path} -u {self.mysqlusername} -p{self.mysqlpassword} -h {self.mysqlhost} --port={self.mysqlport} < {sql_file_path}"
                subprocess.run(command, shell=True, check=True)

            elif self.location_type == 'remote':
                attempt = 0
                while attempt < self.max_attempts:
                    try:
                        transfer = self.get_transfer()
                        # 上传时，远程要求是文件夹
                        transfer.upload(sql_file_path, "/tmp/")
                        # 导入时要求是文件下的数据文件
                        remote_sql_path = f"/tmp/{os.path.basename(sql_file_path)}"
                        remote_command = f"{self.remote_mysql_path} -u {self.mysqlusername} -p{self.mysqlpassword} -h {self.mysqlhost} --port={self.mysqlport} < {remote_sql_path}"
                        _, _, error = self.execute_ssh_command(remote_command)
                        
                        # 检查 error 变量
                        if error and "error" in error.lower():
                            raise Exception(error)
                        
                        # 删除临时文件
                        delete_command = f"sudo rm {remote_sql_path}"
                        self.execute_ssh_command(delete_command)
                        logging.info(f"All databases imported successfully from remote SQL file.")
                        break
                    except Exception as e:
                        attempt += 1
                        logging.error(f"Error occurred while importing the database from remote: {e}")
                        if attempt < self.max_attempts:
                            logging.info(f"Retrying... ({attempt}/{self.max_attempts})")
                            sleep(self.sleep_time)
                        else:
                            logging.error("Max retries reached. Failed to import database.")
                            raise
            else:
                logging.error(f"Invalid location_type: {self.location_type}")
                return
        
        except Exception as e:
            logging.error(f"Error occurred while importing all databases from SQL file: {e}")
            raise
    
    def export_table_to_dataframe(self, database_name, table_name=None, query=None):
        try:
            with self.engine.connect() as connection:
                connection.execute(text(f"USE {database_name}")) 
                if query:
                    logging.warning("Both table name anquery provided. Ignoring the table name and using the provided query.")
                    final_query = query
                elif table_name:
                    final_query = f"SELECT * FROM {table_name}"
                else:
                    logging.error("Either table name or query must be provided.")
                    return None

                df = pd.read_sql(final_query, connection)
                logging.info("Data exported successfully.")
                return df
        except SQLAlchemyError as e:
            logging.error(f"Error occurred while importing data: {e}")
            return None