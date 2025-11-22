import pytest
import psycopg2

@pytest.fixture(scope="module")
def db_conn():
    conn = psycopg2.connect(dbname="banking", user="airflow", password="airflow", host="localhost")
    yield conn
    conn.close()
