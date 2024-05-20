下面是如何使用 SQLAlchemy 和 Alembic 进行数据库迁移的示例：
### 1. 安装 Alembic

首先，安装 Alembic：

```bash
pip install alembic
```


### 2. 初始化 Alembic

在你的项目目录中，初始化 Alembic：

```bash
alembic init alembic
```



这将创建一个 `alembic` 目录，包含 Alembic 的配置文件和脚本模板。
### 3. 配置 Alembic

编辑 `alembic.ini` 文件，配置数据库连接字符串：

```ini
# 在 alembic.ini 文件中找到以下行并修改为你的数据库连接字符串
sqlalchemy.url = mysql+pymysql://username:password@localhost:3306/test_db
```



在 `alembic/env.py` 中，配置 SQLAlchemy 的 `Base`：

```python
from myapp import Base  # 导入你的 Base 对象

# 在 run_migrations_online 函数中找到以下行
target_metadata = Base.metadata
```


### 4. 创建模型

定义你的 SQLAlchemy 模型，例如在 `models.py` 中：

```python
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False)
    email = Column(String(100), unique=True, nullable=False)
```


### 5. 生成迁移文件

运行 Alembic 生成初始迁移文件：

```bash
alembic revision --autogenerate -m "Initial migration"
```



这将生成一个新的迁移文件，其中包含创建 `User` 表的代码。
### 6. 应用迁移

运行 Alembic 迁移，创建数据库表：

```bash
alembic upgrade head
```


### 7. 使用 DatabaseManager

下面是更新后的 `DatabaseManager` 类和使用 Alembic 迁移的示例：

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import logging
from models import User, Base  # 假设你的模型文件名为 models.py

class DatabaseManager:
    def __init__(self, username, password, host, port, database):
        self.connection_string = f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}'
        self.engine = create_engine(self.connection_string)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def add_user(self, name, email):
        try:
            new_user = User(name=name, email=email)
            self.session.add(new_user)
            self.session.commit()
            logging.info(f"User '{name}' added successfully.")
        except SQLAlchemyError as e:
            self.session.rollback()
            logging.error(f"Error occurred while adding the user: {e}")
            raise

    def get_user(self, user_id):
        try:
            user = self.session.query(User).filter(User.id == user_id).one()
            return user
        except SQLAlchemyError as e:
            logging.error(f"Error occurred while fetching the user: {e}")
            raise

# Example usage
if __name__ == "__main__":
    db_manager = DatabaseManager(username='root', password='password', host='localhost', port=3306, database='test_db')
    db_manager.add_user('Alice', 'alice@example.com')
    user = db_manager.get_user(1)
    print(f"User fetched: {user.name}, {user.email}")
```


### 说明 
- **Alembic 配置** ：配置 Alembic 以使用你的数据库连接和模型元数据。 
- **迁移管理** ：通过 Alembic 生成和应用数据库迁移文件，以版本控制的方式管理数据库架构变更。 
- ** 类** ：连接和管理数据库操作，避免直接创建表。

通过这种方法，你可以在项目中使用 Alembic 进行数据库迁移管理，同时使用 SQLAlchemy 进行 ORM 操作，达到更好的模块化和管理数据库变更的效果。


