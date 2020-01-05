import setuptools

# requirements:
# airflow
# airflow-fs-0.1.0
# pip install psycopg2
# pip install mysql-connector-python-8.0.18

# for testing:
# pip install pytest_docker_tools
# pip install pytest-mock

setuptools.setup(
    author="Bas Harenslak",
    description="Composable database hooks and operators for Airflow.",
    keywords="airflow_db",
    name="airflow_db",
    version="0.2.0",
    packages=setuptools.find_packages("src"),
    package_dir={"": "src"},
    test_suite="tests",
)
