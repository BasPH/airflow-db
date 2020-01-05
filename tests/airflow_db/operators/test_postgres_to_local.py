import csv
import json
import os

from airflow.models import Connection
from airflow_db.hooks.postgres import PostgresHook
from airflow_db.operators.db_to_fs import DbToFsOperator
from airflow_fs.hooks import S3Hook, LocalHook
from pytest_docker_tools import container, fetch, network
from s3fs import S3FileSystem

postgres_image = fetch(repository="postgres:12.1-alpine")
s3_image = fetch(repository="minio/minio:RELEASE.2019-12-30T05-45-39Z")
s3_init_image = fetch(repository="minio/mc:RELEASE.2019-08-29T00-40-57Z")
docker_network = network(name="testnetwork")

postgres = container(
    image="{postgres_image.id}",
    environment={"POSTGRES_USER": "testuser", "POSTGRES_PASSWORD": "testpass"},
    ports={"5432/tcp": None},
    volumes={
        os.path.join(os.path.dirname(__file__), "postgres-init.sql"): {
            "bind": "/docker-entrypoint-initdb.d/postgres-init.sql"
        },
        os.path.join(os.path.dirname(__file__), "testdata.csv"): {
            "bind": "/docker-entrypoint-initdb.d/testdata.csv"
        },
    },
)


def test_postgres_to_local_csv(mocker, postgres, tmp_path):
    mocker.patch.object(
        PostgresHook,
        "get_connection",
        return_value=Connection(
            conn_id="test",
            conn_type="postgres",
            host="localhost",
            login="testuser",
            password="testpass",
            port=postgres.ports["5432/tcp"][0],
        ),
    )

    task = DbToFsOperator(
        task_id="test_id",
        src_db_hook=PostgresHook(conn_id="this_conn_will_be_mocked"),
        src_query="SELECT * FROM dummy",
        output_filetype="csv",
        dest_path=str(tmp_path / "test.csv"),
        dest_fs_hook=LocalHook(),
    )
    task.execute(context={})

    # Assert output
    with open(os.path.join(os.path.dirname(__file__), "testdata.csv")) as local_file:
        csv_reader = csv.reader(local_file)
        local_data = list(csv_reader)

    with (tmp_path / "test.csv").open(mode="r") as result_file:
        csv_reader = csv.reader(result_file)
        result_data = list(csv_reader)

    assert local_data == result_data


def test_postgres_to_local_json(mocker, postgres, tmp_path):
    mocker.patch.object(
        PostgresHook,
        "get_connection",
        return_value=Connection(
            conn_id="test",
            conn_type="postgres",
            host="localhost",
            login="testuser",
            password="testpass",
            port=postgres.ports["5432/tcp"][0],
        ),
    )

    task = DbToFsOperator(
        task_id="test_id",
        src_db_hook=PostgresHook(conn_id="this_conn_will_be_mocked"),
        src_query="SELECT * FROM dummy",
        output_filetype="json",
        dest_path=str(tmp_path / "test.json"),
        dest_fs_hook=LocalHook(),
    )
    task.execute(context={})

    # Assert output
    with open(os.path.join(os.path.dirname(__file__), "testdata.csv")) as local_file:
        csv_reader = csv.reader(local_file)
        header = next(csv_reader)
        rows = list(csv_reader)
        local_data = [{header[i]: col for i, col in enumerate(row)} for row in rows]  # convert to dict

    with (tmp_path / "test.json").open(mode="r") as result_file:
        result_data = json.load(result_file)

    # CSV reader makes everything string, so cast all columns to string for comparing values
    result_data = [{str(k): str(v) for k, v in row.items()} for row in result_data]
    assert local_data == result_data
