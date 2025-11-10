# scripts/create_aggregate.py
from db import create_connection

def create_aggregate_table(cur, conn):
    q = """
    CREATE TABLE IF NOT EXISTS songplays_daily (
        date DATE PRIMARY KEY,
        play_count INT
    );
    """
    cur.execute(q)
    conn.commit()

    # populate
    cur.execute("""
    INSERT INTO songplays_daily (date, play_count)
    SELECT DATE(start_time) as date, COUNT(*) as play_count
    FROM songplays
    GROUP BY DATE(start_time)
    ON DUPLICATE KEY UPDATE play_count = VALUES(play_count);
    """)
    conn.commit()
    print("Aggregate table created/updated.")

def main():
    cur, conn = create_connection()
    create_aggregate_table(cur, conn)
    conn.close()
    print("Create aggregate done.")

if __name__ == "__main__":
    main()
