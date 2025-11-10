
from db import create_connection
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    for q in drop_table_queries:
        try:
            cur.execute(q)
            conn.commit()
            print(f"Dropped: {q.split()[2]}")
        except Exception as e:
            print(f"Error dropping table: {e}")

def create_tables(cur, conn):
    for q in create_table_queries:
        try:
            cur.execute(q)
            conn.commit()
            print("Created table.")
        except Exception as e:
            print(f"Error creating table: {e}")

def main():
    cur, conn = create_connection()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()
    print("Done create_tables")

if __name__ == "__main__":
    main()
