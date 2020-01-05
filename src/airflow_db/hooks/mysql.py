from typing import Tuple, List, Dict

import mysql.connector
from airflow_db.hooks.db import DbHook


class MysqlHook(DbHook):
    def __init__(self, conn_id):
        super().__init__()
        self._conn_id = conn_id
        self._cursor_kwargs = {"dictionary": True}

    def get_conn(self):
        """Instantiate and cache a connection to the Postgres database."""
        if self._conn is None:
            conn_config = self.get_connection(self._conn_id)
            conn_kwargs = {
                "host": conn_config.host,
                "user": conn_config.login,
                "password": conn_config.password,
                "database": conn_config.schema,
                "port": conn_config.port,
            }
            self._conn = mysql.connector.connect(**conn_kwargs)

        return self._conn

    def get_records(self, query: str) -> Tuple[List, List[Dict]]:
        """
        Return the records for the given query.

        For dealing with the results in a consistent matter, we assume a dict is returned by the cursor. The
        dict-reading-class is database-specific and should therefore be set in the hook for the specific
        database.

        :param query:
        :return:
        """
        cursor = self.get_conn().cursor(**self._cursor_kwargs)
        cursor.execute(query)
        records = cursor.fetchall()
        header = [_[0] for _ in cursor.description]
        cursor.close()
        return header, records
