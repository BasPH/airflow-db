import csv
import json
import logging
from typing import TextIO

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow_db.hooks.db import DbHook
from airflow_fs.hooks import LocalHook, FsHook


class DbToFsOperator(BaseOperator):
    """Operator for querying a database and storing the result on a filesystem."""

    template_fields = ("_src_query", "_dest_path")

    @apply_defaults
    def __init__(
        self,
        src_db_hook: DbHook,
        src_query: str,
        output_filetype: str,
        dest_path: str,
        dest_fs_hook: FsHook = None,
        **kwargs
    ):
        super().__init__(**kwargs)

        self._src_db_hook = src_db_hook
        self._src_query = src_query
        self._output_filetype = output_filetype
        self._dest_path = dest_path
        self._dest_fs_hook = dest_fs_hook or LocalHook()

    def execute(self, context):
        # with self._src_db_hook as src_db_hook:
        #     header, records = src_db_hook.get_records(self._src_query)
        header, records = self._src_db_hook.get_records(self._src_query)

        if header or records:
            with self._dest_fs_hook as fs_hook:  # type: FsHook
                with fs_hook.open(self._dest_path, "w") as fileio:  # type: TextIO

                    if self._output_filetype == "csv":
                        csv_writer = csv.DictWriter(fileio, fieldnames=header)
                        csv_writer.writeheader()
                        csv_writer.writerows(records)

                    elif self._output_filetype == "json":
                        fileio.write(json.dumps(records))

                    else:
                        logging.exception("Output filetype '%s' not supported.")
