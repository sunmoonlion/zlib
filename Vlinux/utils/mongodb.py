import pymongo
import pandas as pd

class MongoDBDatabase:
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client = None
        self.db = None

    def connect(self):
        try:
            self.client = pymongo.MongoClient(self.host, self.port, username=self.username, password=self.password)
            print("连接成功")
        except pymongo.errors.ConnectionFailure as e:
            print(f"连接失败: {e}")

    def create_database(self, database_name):
        try:
            self.db = self.client[database_name]
            print(f"数据库 '{database_name}' 创建成功！")
        except pymongo.errors.OperationFailure as e:
            print(f"创建数据库时出错: {e}")

    def delete_database(self, database_name):
        try:
            self.client.drop_database(database_name)
            print(f"数据库 '{database_name}' 删除成功！")
        except pymongo.errors.OperationFailure as e:
            print(f"删除数据库时出错: {e}")

    def create_collection(self, collection_name):
        try:
            self.db.create_collection(collection_name)
            print(f"集合 '{collection_name}' 创建成功！")
        except pymongo.errors.OperationFailure as e:
            print(f"创建集合时出错: {e}")

    def delete_collection(self, collection_name):
        try:
            self.db.drop_collection(collection_name)
            print(f"集合 '{collection_name}' 删除成功！")
        except pymongo.errors.OperationFailure as e:
            print(f"删除集合时出错: {e}")

    def insert_data(self, collection_name, data):
        try:
            collection = self.db[collection_name]
            if isinstance(data, list):
                collection.insert_many(data)
            elif isinstance(data, dict):
                collection.insert_one(data)
            print("数据插入成功！")
        except pymongo.errors.OperationFailure as e:
            print(f"插入数据时出错: {e}")

    def find_data(self, collection_name, query=None):
        try:
            collection = self.db[collection_name]
            if query:
                results = collection.find(query)
            else:
                results = collection.find()
            df = pd.DataFrame(list(results))
            print("查询结果:")
            print(df)
        except pymongo.errors.OperationFailure as e:
            print(f"查询数据时出错: {e}")

    def close_connection(self):
        if self.client:
            self.client.close()
            print("连接已关闭")
        else:
            print("没有可关闭的连接")

    # 使用Pandas导出整个集合到DataFrame
    def export_collection_to_dataframe(self, collection_name):
        try:
            collection = self.db[collection_name]
            results = collection.find()
            df = pd.DataFrame(list(results))
            print(f"集合 '{collection_name}' 导出到DataFrame成功！")
            return df
        except pymongo.errors.OperationFailure as e:
            print(f"导出集合到DataFrame时出错: {e}")

if __name__ == "__main__":
    # 创建 MongoDB 对象
    db = MongoDB(host="localhost", port=27017, username="root", password="password")

    # 连接到 MongoDB 服务器
    db.connect()

    # 创建数据库
    db.create_database("example_database")

    # 创建集合
    db.create_collection("example_collection")

    # 插入数据
    data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    db.insert_data("example_collection", data)

    # 查询数据
    db.find_data("example_collection")

    # 导出整个集合到DataFrame
    df = db.export_collection_to_dataframe("example_collection")

    # 进行一些处理或者分析
    # 例如：
    # df['new_column'] = df['old_column'] * 2

    # 删除集合
    db.delete_collection("example_collection")

    # 删除数据库
    db.delete_database("example_database")

    # 关闭连接
    db.close_connection()