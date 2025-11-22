import psycopg2
import pytest

@pytest.fixture(scope="module")
def db_conn():
    conn = psycopg2.connect(dbname="banking", user="airflow", password="airflow", host="localhost")
    yield conn
    conn.close()

def test_insert_and_query(db_conn):
    cur = db_conn.cursor()
    cur.execute("UPDATE accounts SET balance = 1000 where id = 1")
    db_conn.commit()
    cur.execute("SELECT balance FROM accounts WHERE id=1")
    balance = cur.fetchone()[0]
    assert balance == 1000
