# scripts/create_aggregate.py
import sys
import os

# --- Cấu hình để Python tìm thấy file etl_logger.py ở thư mục cha ---
current_dir = os.path.dirname(os.path.abspath(__file__)) # .../transform
parent_dir = os.path.dirname(current_dir)                # .../
sys.path.append(parent_dir)

from db import create_connection
from etl_logger import ETLLogger

def create_aggregate_table(cur, conn):
    # 1. Tạo bảng songplays_daily nếu chưa có
    q = """
    CREATE TABLE IF NOT EXISTS songplays_daily (
        date DATE PRIMARY KEY,
        play_count INT
    );
    """
    cur.execute(q)
    conn.commit()

    # 2. Tính toán và đổ dữ liệu vào (Aggregation)
    query_insert = """
    INSERT INTO songplays_daily (date, play_count)
    SELECT DATE(start_time) as date, COUNT(*) as play_count
    FROM songplays
    GROUP BY DATE(start_time)
    ON DUPLICATE KEY UPDATE play_count = VALUES(play_count);
    """
    cur.execute(query_insert)
    
    # Lấy số dòng được insert/update
    rows_affected = cur.rowcount
    conn.commit()
    
    print(f"Aggregate table created/updated. Rows affected: {rows_affected}")
    return rows_affected

def main():
    # Khởi tạo Logger
    logger = ETLLogger("transform.create_aggregate")
    logger.start()

    cur, conn = create_connection()
    
    try:
        # Chạy logic chính
        rows = create_aggregate_table(cur, conn)
        conn.close()
        
        # Ghi log thành công
        # Với bước transform: extracted = loaded = số dòng tạo ra
        logger.log_success(extracted=rows, loaded=rows, rejected=0)
        print("Create aggregate done.")
        
    except Exception as e:
        # Ghi log thất bại
        print(f"Error: {e}")
        logger.log_fail(str(e))
        if conn:
            conn.close()
        raise

if __name__ == "__main__":
    main()