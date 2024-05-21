from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import logging
from models import User, Base  # 假设你的模型文件名为 models.py

class MySQLdatabaseORM:
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
    db_manager = MySQLdatabaseORM(username='root', password='password', host='localhost', port=3306, database='test_db')
    db_manager.add_user('Alice', 'alice@example.com')
    user = db_manager.get_user(1)
    print(f"User fetched: {user.name}, {user.email}")
