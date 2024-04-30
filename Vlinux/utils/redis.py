# class RedisDatabasee导入模块
import redis
import pandas as pd

class RedisDatabase:
    def __init__(self, host, port, password=None, db=0):
        """
        初始化 RedisDatabase 类的实例。
        
        参数:
        - host: Redis 服务器的主机名或 IP 地址。
        - port: Redis 服务器的端口号。
        - password: Redis 服务器的密码，如果没有密码则为 None。
        - db: 要连接的 Redis 数据库编号，默认为 0。
        """
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.connection = None

    def connect(self):
        """
        连接到 Redis 服务器。
        """
        try:
            self.connection = RedisDatabase.StrictRedis(host=self.host, port=self.port, password=self.password, db=self.db)
            print(f"连接成功，当前操作的数据库为 {self.db}")
        except RedisDatabase.ConnectionError as e:
            print(f"连接失败: {e}")

    def insert_data(self, key, data, method="push"):
        """
        向 Redis 集合中插入数据。
        
        参数:
        - key: 要插入数据的键（集合名称）。
        - data: 要插入的数据，可以是单个数据或数据列表。
        - method: 插入数据的方式，可以是 "push"（默认）或 "set"。
        """
        try:
            if method == "push":
                if isinstance(data, list):
                    for item in data:
                        self.connection.rpush(key, item)
                else:
                    self.connection.rpush(key, data)
            elif method == "set":
                self.connection.set(key, data)
            else:
                print("不支持的插入方式")
                return
            print("数据插入成功！")
        except RedisDatabase.RedisError as e:
            print(f"插入数据时出错: {e}")

    def get_data(self, key):
        """
        查询 Redis 集合中的数据。
        
        参数:
        - key: 要查询数据的键（集合名称）。
        
        返回值:
        - 查询结果
        """
        try:
            return self.connection.lrange(key, 0, -1)
        except RedisDatabase.RedisError as e:
            print(f"查询数据时出错: {e}")
            return []

    def convert_to_dataframe(self, data):
        """
        将查询到的数据转换为 DataFrame 格式。
        
        参数:
        - data: 查询到的数据，可以是单个键的查询结果，也可以是多个键的查询结果列表
        
        返回值:
        - DataFrame 格式的数据
        """
        try:
            if isinstance(data, list):
                # 多个键的查询结果列表，将所有结果合并为一个列表
                combined_data = []
                for item in data:
                    combined_data.extend(item)
                df = pd.DataFrame(combined_data)
                print("多键查询结果转换为 DataFrame，每个键对应一列数据")
            else:
                # 单个键的查询结果，直接转换为 DataFrame
                df = pd.DataFrame(data)
                print("单键查询结果转换为 DataFrame，只有一列数据")
            return df
        except Exception as e:
            print(f"转换数据为 DataFrame 时出错: {e}")
            return pd.DataFrame()

    def close_connection(self):
        """
        关闭与 Redis 服务器的连接。
        """
        if self.connection:
            self.connection.close()
            print("连接已关闭")
        else:
            print("没有可关闭的连接")

# 示例用法
"""
# 创建 RedisDatabase 对象，并连接到 Redis 服务器
db = RedisDatabase(host="localhost", port=6379, password="password", db=0)
db.connect()

# 插入数据
data1 = ["Alice", "Bob", "Charlie"]
data2 = ["David", "Eva", "Frank"]
db.insert_data("example_key1", data1, method="push")
db.insert_data("example_key2", data2, method="push")

# 单键查询数据
result1 = db.get_data("example_key1")
# 将查询到的数据转换为 DataFrame
df1 = db.convert_to_dataframe(result1)
print("单键查询结果1:")
print(df1)

# 多键查询数据
result2 = [db.get_data("example_key1"), db.get_data("example_key2")]
# 将查询到的数据转换为 DataFrame
df2 = db.convert_to_dataframe(result2)
print("多键查询结果2:")
print(df2)

# 关闭连接
db.close_connection()
"""
