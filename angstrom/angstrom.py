import sqlite3
from os import path


def mobj(mapping, result_set):
    rows = []
    for row in result_set:
        new_row = mapping.copy()
        for key, value in mapping.items():
            new_row[key] = row[value]
        rows.append(new_row)
    return rows

def base_connector(db_name):
    def connect():
        conn = sqlite3.connect(db_name)
        conn.row_factory = sqlite3.Row
        return conn
    return connect

def file_system_sql_loader(sql_path):
    def get_sql(filename):
        with open(path.join(sql_path, filename)) as _file:
            return _file.read()
    return get_sql

class TransactionManager:

    def __init__(self, connector):
        self._connector = connector
        self._conn = None

    def __enter__(self):
        self._conn = self._connector()
        return self._conn

    def __exit__(self, type, value, tb):
        if tb:
            self._conn.rollback()
        else:
            self._conn.commit()
        self._conn.close()

class Db:

    def __init__(self, connector, loader):
        self._connect = connector
        self._get_sql = loader

    def start_transaction(self):
        return TransactionManager(self._connect)

    def execute_script(self, name):
        with self._connect() as conn:
            sql = self._get_sql(name)
            conn.executescript(sql)

    def execute_many(self, name, parameter_list, conn=None):
        if conn:
            self._execute_many(name, parameter_list, conn)
        else:
            with self._connect() as conn:
                self._execute_many(name, parameter_list, conn)

    def _execute_many(self, name, parameter_list, conn):
        sql = self._get_sql(name)
        conn.executemany(sql, parameter_list)

    def execute_query(self, name, parameters=None, conn=None):
        if conn:
            return self._execute_query(name, conn, parameters)
        else:
            with self._connect() as conn:
                return self._execute_query(name, conn, parameters)

    def _execute_query(self, name, conn, parameters=None):
        sql = self._get_sql(name)
        cursor = conn.cursor()
        if parameters:
            cursor.execute(sql, parameters)
        else:
            cursor.execute(sql)
        return cursor.fetchall()
