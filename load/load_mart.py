# scripts/load_mart.py
from db import create_connection

def load_datamart(cur, conn):
    # Example: create mart table and populate from songplays_daily
    cur.execute("""
    CREATE TABLE IF NOT EXISTS mart_daily_plays (
        date DATE PRIMARY KEY,
        total_plays INT
    );
    """)
    conn.commit()
    cur.execute("""
    INSERT INTO mart_daily_plays (date, total_plays)
    SELECT date, play_count FROM songplays_daily
    ON DUPLICATE KEY UPDATE total_plays = VALUES(total_plays);
    """)
    conn.commit()
    print("Datamart loaded/updated.")

def main():
    cur, conn = create_connection()
    load_datamart(cur, conn)
    conn.close()
    print("Load mart done.")

if __name__ == "__main__":
    main()
