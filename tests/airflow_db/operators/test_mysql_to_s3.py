import csv
import json
import os

import pytest
from airflow.models import Connection
from airflow_db.hooks.mysql import MysqlHook
from airflow_db.operators.db_to_fs import DbToFsOperator
from airflow_fs.hooks import S3Hook
from pytest_docker_tools import container, fetch, network
from s3fs import S3FileSystem

mysql_image = fetch(repository="mysql:8.0.18")
s3_image = fetch(repository="minio/minio:RELEASE.2019-12-30T05-45-39Z")
s3_init_image = fetch(repository="minio/mc:RELEASE.2019-08-29T00-40-57Z")
docker_network = network(name="testnetwork")

mysql = container(
    image="{mysql_image.id}",
    environment={
        "MYSQL_DATABASE": "testdb",
        "MYSQL_USER": "testuser",
        "MYSQL_PASSWORD": "testpass",
        "MYSQL_ROOT_PASSWORD": "supersecret",
    },
    ports={"3306/tcp": None},
    volumes={
        os.path.join(os.path.dirname(__file__), "mysql-init.sql"): {
            "bind": "/docker-entrypoint-initdb.d/mysql-init.sql"
        },
        os.path.join(os.path.dirname(__file__), "testdata.csv"): {
            "bind": "/docker-entrypoint-initdb.d/testdata.csv"
        },
    },
    command="--secure-file-priv=/docker-entrypoint-initdb.d",
)

s3 = container(
    image="{s3_image.id}",
    name="s3",
    ports={"9000/tcp": None},
    environment={"MINIO_ACCESS_KEY": "secretaccess", "MINIO_SECRET_KEY": "secretkey"},
    command="server /data",
    network="{docker_network.name}",
)

s3_init = container(
    image="{s3_init_image.id}",
    name="s3_init",
    entrypoint="/bin/sh",
    command=(
        "-c '"
        "while ! nc -z s3 9000; do echo Waiting 1 sec for s3 to be healthy... && sleep 1; done;"
        "echo Made connection.;"
        "/usr/bin/mc config host add test_s3 http://{s3.name}:9000 secretaccess secretkey;"
        "/usr/bin/mc mb test_s3/testbucket;"
        "exit 0'"
    ),
    network="{docker_network.name}",
)


@pytest.fixture(autouse=True)
def mysql_and_s3_mocks(mocker, mysql, s3):
    mocker.patch.object(
        MysqlHook,
        "get_connection",
        return_value=Connection(
            conn_id="test",
            conn_type="mysql",
            host="localhost",
            login="testuser",
            password="testpass",
            schema="testdb",
            port=mysql.ports["3306/tcp"][0],
        ),
    )

    mocker.patch.object(
        S3Hook,
        "get_conn",
        return_value=S3FileSystem(
            key="secretaccess",
            secret="secretkey",
            client_kwargs={"endpoint_url": f'http://localhost:{s3.ports["9000/tcp"][0]}'},
        ),
    )


def test_mysql_to_s3_csv(mocker, mysql, s3, s3_init):
    task = DbToFsOperator(
        task_id="test_id",
        src_db_hook=MysqlHook(conn_id="this_conn_will_be_mocked"),
        src_query="SELECT * FROM dummy",
        output_filetype="csv",
        dest_path="s3://testbucket/test.csv",
        dest_fs_hook=S3Hook(conn_id="this_conn_will_be_mocked_2"),
    )
    task.execute(context={})

    # Assert output
    with open(os.path.join(os.path.dirname(__file__), "testdata.csv")) as local_file:
        csv_reader = csv.reader(local_file)
        local_data = list(csv_reader)

    s3fs = S3Hook().get_conn()
    with s3fs.open("s3://testbucket/test.csv", mode="r") as s3_file:
        csv_reader = csv.reader(s3_file)
        s3_data = list(csv_reader)

    assert local_data == s3_data


def test_mysql_to_s3_json(mocker, mysql, s3, s3_init):
    task = DbToFsOperator(
        task_id="test_id",
        src_db_hook=MysqlHook(conn_id="this_conn_will_be_mocked"),
        src_query="SELECT * FROM dummy",
        output_filetype="json",
        dest_path="s3://testbucket/test.json",
        dest_fs_hook=S3Hook(conn_id="this_conn_will_be_mocked_2"),
    )
    task.execute(context={})

    # Assert output
    with open(os.path.join(os.path.dirname(__file__), "testdata.csv")) as local_file:
        csv_reader = csv.reader(local_file)
        header = next(csv_reader)
        rows = list(csv_reader)
        local_data = [{header[i]: col for i, col in enumerate(row)} for row in rows]  # convert to dict

    s3fs = S3Hook().get_conn()
    with s3fs.open("s3://testbucket/test.json", mode="r") as s3_file:
        s3_data = json.load(s3_file)

    # CSV reader makes everything string, so cast all columns to string for comparing values
    s3_data = [{str(k): str(v) for k, v in row.items()} for row in s3_data]
    assert local_data == s3_data
