from sqlalchemy import create_engine, text
import logging

class UserManager:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)

    def create_user_and_grant_privileges(self, new_user, new_user_password, pri_database='*', pri_table='*', pri_host='%'):
        try:
            with self.engine.connect() as conn:
                print(new_user)
                # 使用正确的字符串拼接方式，确保 pri_host 中的 '%' 不会被错误处理
                create_user_sql = f"CREATE USER IF NOT EXISTS '{new_user}'@'{pri_host}' IDENTIFIED BY '{new_user_password}'"
                grant_privileges_sql = f"GRANT ALL PRIVILEGES ON {pri_database}.{pri_table} TO '{new_user}'@'{pri_host}'"
                
                # 打印调试信息
                print(f"Executing SQL: {create_user_sql}")
                print(f"Executing SQL: {grant_privileges_sql}")

                conn.execute(text(create_user_sql))
                conn.execute(text(grant_privileges_sql))
                conn.execute(text("FLUSH PRIVILEGES"))
                
            logging.info(f"User '{new_user}' created and granted privileges successfully.")
        except Exception as e:
            logging.error(f"Error occurred while creating the user and granting privileges: {e}")
            raise

# 使用具有足够权限的连接字符串
connection_string = 'mysql+pymysql://root:your_root_password@localhost/your_database'
user_manager = UserManager(connection_string)

# 创建用户并授予权限
user_manager.create_user_and_grant_privileges('repl_user', 'repl_password')
