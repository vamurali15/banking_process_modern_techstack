def test_insert_and_query(db_conn):
    cur = db_conn.cursor()

    # Insert row if not exists (or you can delete and insert fresh)
    cur.execute("INSERT INTO accounts (id, balance) VALUES (1, 0) ON CONFLICT (id) DO NOTHING")
    db_conn.commit()

    # Update the balance
    cur.execute("UPDATE accounts SET balance = 1000 WHERE id = 1")
    db_conn.commit()

    # Query the updated balance
    cur.execute("SELECT balance FROM accounts WHERE id=1")
    balance = cur.fetchone()[0]

    assert balance == 1000
