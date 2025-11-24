# scripts/load/load_mart.py
import sys
import os

# Setup đường dẫn
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from db import create_connection_Mart
from etl_logger import ETLLogger
from sql_queries import (
    mart_top_artists_insert,
    mart_overview_queries
)

def load_datamart(cur, conn):
    print("--- Đang tải dữ liệu vào Data Mart ---")
    
    # 1. Cập nhật bảng Top Artists (chi tiết + hit song)
    print("1. Đang tính toán và cập nhật Top Artists (bao gồm Top Hit Song)...")
    try:
        cur.execute(mart_top_artists_insert)
        conn.commit()
        print(f"   -> Thành công. Số dòng ảnh hưởng: {cur.rowcount}")
    except Exception as e:
        print(f"   -> Lỗi cập nhật Top Artists: {e}")

    # 2. Cập nhật bảng Overview
    print("2. Đang cập nhật System Overview...")
    try:
        for q in mart_overview_queries:
            cur.execute(q)
        conn.commit()
        print("   -> Thành công.")
    except Exception as e:
        print(f"   -> Lỗi cập nhật Overview: {e}")

def main():
    logger = ETLLogger("load.load_mart")
    logger.start()
    
    cur = None
    conn = None
    try:
        cur, conn = create_connection_Mart()
        load_datamart(cur, conn)
        
        logger.log_success(extracted=0, loaded=0, rejected=0)
        print("=== Load Data Mart Hoàn Tất ===")
        
    except Exception as e:
        print(f"Lỗi Critical Load Mart: {e}")
        logger.log_fail(str(e))
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    main()