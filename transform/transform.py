# scripts/transform.py
import pandas as pd
from db import create_connection

def create_songplay_summary(cur, conn):
    """
    Ví dụ transform: tổng số songplays theo ngày, top artists, ...
    Kết quả ghi vào bảng aggregate trong DB hoặc file.
    """
    # simple query to get songplays with start_time date
    q = """
    SELECT DATE(start_time) as dt, COUNT(*) as play_count
    FROM songplays
    GROUP BY DATE(start_time);
    """
    cur.execute(q)
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["date", "play_count"])
    # save to CSV in staging transform folder
    out = "data/transform/songplays_summary.csv"
    import os
    os.makedirs(os.path.dirname(out), exist_ok=True)
    df.to_csv(out, index=False)
    print(f"Transform saved to {out}")

def main():
    cur, conn = create_connection()
    create_songplay_summary(cur, conn)
    conn.close()
    print("Transform done.")

if __name__ == "__main__":
    main()
