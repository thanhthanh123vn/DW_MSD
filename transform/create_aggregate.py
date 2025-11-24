# transform/create_aggregate.py
import sys
import os

# Cấu hình import
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from db import create_connection_Mart
from etl_logger import ETLLogger

def create_aggregate_table(cur, conn):
    print("Đang tính toán Songplays Daily...")
    
    # 1. Insert vào bảng trong MART (kết nối hiện tại)
    # 2. Select từ bảng trong WAREHOUSE (dùng cú pháp database_warehouse.table)
    query_insert = """
    INSERT INTO songplays_daily (date, play_count)
    SELECT 
        DATE(start_time) as date, 
        COUNT(*) as play_count
    FROM database_warehouse.songplays
    GROUP BY DATE(start_time)
    ON DUPLICATE KEY UPDATE 
        play_count = VALUES(play_count),
        updated_at = NOW();
    """
    
    cur.execute(query_insert)
    rows_affected = cur.rowcount
    conn.commit()
    
    print(f" -> Đã cập nhật songplays_daily. Số dòng ảnh hưởng: {rows_affected}")
    return rows_affected

def main():
    logger = ETLLogger("transform.create_aggregate")
    logger.start()

    # Kết nối tới MART vì đây là nơi chứa bảng kết quả (songplays_daily)
    cur, conn = create_connection_Mart()
    
    try:
        rows = create_aggregate_table(cur, conn)
        conn.close()
        
        logger.log_success(extracted=rows, loaded=rows, rejected=0)
        print("Create aggregate done.")
        
    except Exception as e:
        print(f"Error: {e}")
        logger.log_fail(str(e))
        if conn: conn.close()
        raise

if __name__ == "__main__":
    main()