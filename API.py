import pandas as pd
from sqlalchemy import create_engine, inspect, text
import time


def timer(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Время выполнения функции: {time.strftime('%H:%M:%S', time.gmtime(execution_time))}")
        return result
    return wrapper


class Retake:
    def __init__(self, host, username, password, database):
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.engine = create_engine(f"postgresql+psycopg2://{username}:{password}@{host}/{database}")

    @timer
    def create_table(self, df, table):
        inspector = inspect(self.engine)
        if table not in inspector.get_table_names():
            df.to_sql(table, self.engine, if_exists='append', index=False)
            print(f'CREATE_TABLE таблицы "{table}" выполнена.')
        else:
            print(f'Таблица "{table}" уже существует в базе данных.')

    @timer
    def delete_from_table(self, table, conditions):
        inspector = inspect(self.engine)
        if table not in inspector.get_table_names():
            print(f'Таблица "{table}" не существует в базе данных.')
        else:
            with self.engine.connect() as conn:
                query = f"DELETE FROM {table} WHERE {conditions}"
                conn.execute(text(query))
                conn.commit()
                print(f"DELETE_FROM_TABLE в таблице {table} выполнено.")

    @timer
    def truncate_table(self, table):
        inspector = inspect(self.engine)
        if table not in inspector.get_table_names():
            print(f'Таблица "{table}" не существует в базе данных.')
        else:
            with self.engine.connect() as conn:
                query = f"TRUNCATE TABLE {table}"
                conn.execute(text(query))
                conn.commit()
                print(f'Команда TRUNCATE TABLE Таблицы "{table}" выполнено.')

    @timer
    def read_sql(self, table):
        inspector = inspect(self.engine)
        if table not in inspector.get_table_names():
            print(f'Таблицы "{table}" не существует в базе данных.')
        else:
            query = f"SELECT * FROM {table}"
            result = pd.read_sql_query(query, self.engine)
            return result

    @timer
    def insert_sql(self, df, table):
        inspector = inspect(self.engine)
        if table not in inspector.get_table_names():
            print(f'Таблицы "{table}" не существует в базе данных.')
        else:
            df.to_sql(table, self.engine, if_exists='append', index=False)
            print(f'INSERT_SQL в таблицу "{table}" выполнено')

    @timer
    def execute(self, query):
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            conn.commit()
            return result

