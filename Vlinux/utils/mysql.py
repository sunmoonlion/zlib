# 导入模块
import pymysql
import pandas as pd

# 定义MySQLDatabase类
class MysqlDatabase:
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                charset="utf8mb4"
            )
            self.cursor = self.connection.cursor()
            print("连接成功")
        except pymysql.Error as e:
            print("连接失败:", e)

    def create_database(self, database_name, charset='utf8mb4', collation='utf8mb4_unicode_ci'):
        try:
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name} CHARACTER SET {charset} COLLATE {collation}")
            print(f"数据库 '{database_name}' 创建成功！")
        except pymysql.Error as e:
            print(f"创建数据库时出错: {e}")

    def delete_database(self, database_name):
        try:
            self.cursor.execute(f"DROP DATABASE IF EXISTS {database_name}")
            print(f"数据库 '{database_name}' 删除成功！")
        except pymysql.Error as e:
            print(f"删除数据库时出错: {e}")

    def create_table(self, database_name, table_name, fields):
        try:
            self.cursor.execute(f"USE {database_name}")
            field_definitions = ', '.join(fields)
            self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({field_definitions})")
            print(f"表 '{table_name}' 创建成功！")
        except pymysql.Error as e:
            print(f"创建表时出错: {e}")

    def delete_table(self, database_name, table_name):
        try:
            self.cursor.execute(f"USE {database_name}")
            self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"表 '{table_name}' 删除成功！")
        except pymysql.Error as e:
            print(f"删除表时出错: {e}")

    def insert_data(self, database_name, table_name, data, columns=None):
        try:
            self.cursor.execute(f"USE {database_name}")
            if columns:
                columns_str = ', '.join(columns)
                placeholders = ', '.join(['%s'] * len(columns))
                query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            else:
                placeholders = ', '.join(['%s'] * len(data[0]))
                query = f"INSERT INTO {table_name} VALUES ({placeholders})"
            self.cursor.executemany(query, data)
            self.connection.commit()
            print("数据插入成功！")
        except pymysql.Error as e:
            self.connection.rollback()
            print(f"插入数据时出错: {e}")

    def select_data(self, database_name, table_name):
        try:
            self.cursor.execute(f"USE {database_name}")
            self.cursor.execute(f"SELECT * FROM {table_name}")
            results = self.cursor.fetchall()
            print("查询结果:")
            for row in results:
                print(row)
        except pymysql.Error as e:
            print(f"查询数据时出错: {e}")

    def close_connection(self):
        if self.connection and self.cursor:
            self.cursor.close()
            self.connection.close()
            print("连接已关闭")
        else:
            print("没有可关闭的连接")
    
    # 使用Pandas导入整张表或者查询结果到DataFrame
    def import_table_to_dataframe(self, database_name, table_name=None, query=None):
        try:
            self.cursor.execute(f"USE {database_name}")
            if query and not table_name:
                pass
            elif table_name:
                if query:
                    print("警告：同时提供了表名和查询语句。将忽略查询语句并导入整张表的数据。")
                    query = f"SELECT * FROM {table_name}"
                else:
                    query = f"SELECT * FROM {table_name}"
            else:
                print("错误：必须提供表名或查询语句之一。")
                return None

            df = pd.read_sql(query, self.connection)
            print("数据导入成功！")
            return df
        except pymysql.Error as e:
            print(f"导入数据时出错: {e}")
            return None

    # 使用Pandas导出整张表
    def export_dataframe_to_table(self, dataframe, database_name, table_name):
        try:
            self.cursor.execute(f"USE {database_name}")
            dataframe.to_sql(table_name, self.connection, if_exists='replace', index=False)
            print(f"DataFrame 数据导出到表 '{table_name}' 成功！")
        except pymysql.Error as e:
            print(f"导出DataFrame到表时出错: {e}")


if __name__ == "__main__":
    # 创建MySQLDatabase对象
    db = MySQLDatabase(host="localhost", user="root", password="password")

    # 连接到数据库服务器
    db.connect()

    # 创建数据库
    db.create_database("example_database", charset='utf8', collation='utf8_unicode_ci')

    # 创建表的字段定义
    fields = ["id INT AUTO_INCREMENT PRIMARY KEY", "name VARCHAR(255)", "age INT"]

    # 创建表
    db.create_table(database_name="example_database", table_name="example_table", fields=fields)

    # 插入数据
    data = [(None, 'Alice', 30), (None, 'Bob', 25)]
    columns = ['id', 'name', 'age']  # 指定列名
    db.insert_data(database_name="example_database", table_name="example_table", data=data, columns=columns)

    # 查询数据
    db.select_data(database_name="example_database", table_name="example_table")

    # 导入整张表到DataFrame
    df = db.import_table_to_dataframe(database_name="example_database", table_name="example_table")

    # 进行一些处理或者分析
    # 例如：
    # df['new_column'] = df['old_column'] * 2

    # 将DataFrame导出到指定数据库的表
    db.export_dataframe_to_table(dataframe=df, database_name="example_database", table_name="new_table")

    # 删除表
    db.delete_table(database_name="example_database", table_name="example_table")

    # 删除数据库
    db.delete_database("example_database")

    # 关闭连接
    db.close_connection()