from typing import Tuple, List, Dict, Any

from airflow.hooks.base_hook import BaseHook


class DbHook(BaseHook):
    """Interface defining methods that all database hooks should implement."""

    def __init__(self):
        super().__init__(source=None)
        self._conn = None
        self._cursor_kwargs = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn:
            self._conn.close()

    def get_conn(self):
        raise NotImplementedError()

    def execute(self, query: str) -> None:
        with self.get_conn().cursor() as cursor:
            cursor.execute(query)

    def get_records(self, query: str) -> Tuple[List, Any]:
        """
        Return the records for the given query.

        For dealing with the results in a consistent matter, we expect a dict is returned by the cursor. The
        dict-reading-class is database-specific and should therefore be set in the hook for the specific
        database.

        This method was not implemented because it turned out impossible to write a "generic" function. E.g.
        Postgres implemented context managers, MySQL did not, also Postgres returns record descriptions
        different than MySQL.

        :param query:
        :return:
        """
        raise NotImplementedError()
