from typing import Tuple, List, Any

import psycopg2
from airflow_db.hooks.db import DbHook
from psycopg2.extras import RealDictCursor


class PostgresHook(DbHook):
    def __init__(self, conn_id):
        super().__init__()
        self._conn_id = conn_id
        self._cursor_kwargs = {"cursor_factory": RealDictCursor}

    def get_conn(self):
        """Instantiate and cache a connection to the Postgres database."""
        if self._conn is None:
            conn_config = self.get_connection(self._conn_id)
            conn_kwargs = {
                "host": conn_config.host,
                "user": conn_config.login,
                "password": conn_config.password,
                "dbname": conn_config.schema,
                "port": conn_config.port,
            }
            self._conn = psycopg2.connect(**conn_kwargs)

        return self._conn

    def get_records(self, query: str) -> Tuple[List, Any]:
        """
        Return the records for the given query.

        For dealing with the results in a consistent matter, we assume a dict is returned by the cursor. The
        dict-reading-class is database-specific and should therefore be set in the hook for the specific
        database.

        :param query:
        :return:
        """
        with self.get_conn().cursor(**self._cursor_kwargs) as cursor:
            cursor.execute(query)
            records = cursor.fetchall()
            header = [_.name for _ in cursor.description]
            return header, records
