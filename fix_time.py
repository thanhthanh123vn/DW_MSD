# fix_time.py
import mysql.connector
import pandas as pd
from config import DB_COMMON, DB_NAMES
from sql_queries import time_table_insert

def create_conn(db_key):
    return mysql.connector.connect(
        host=DB_COMMON["host"],
        user=DB_COMMON["user"],
        password=DB_COMMON["password"],
        port=DB_COMMON["port"],
        database=DB_NAMES[db_key],
        autocommit=True
    )

def fix_time_dimension():
    print("--- BẮT ĐẦU ĐỒNG BỘ THỜI GIAN (TIME DIMENSION) ---")
    
    try:
        # 1. Kết nối Warehouse
        conn = create_conn("warehouse")
        cur = conn.cursor()
        
        # 2. Lấy tất cả mốc thời gian từ bảng Songplays
        print("1. Đang quét bảng Songplays để lấy mốc thời gian...")
        cur.execute("SELECT DISTINCT start_time FROM songplays")
        rows = cur.fetchall()
        
        if not rows:
            print("⚠️ Không có dữ liệu trong songplays.")
            return

        print(f"   -> Tìm thấy {len(rows)} mốc thời gian cần xử lý.")

        # 3. Tính toán và Insert vào bảng Time
        print("2. Đang nạp vào bảng Time...")
        count = 0
        
        # Tắt kiểm tra khóa để nạp nhanh
        cur.execute("SET FOREIGN_KEY_CHECKS=0;")
        
        for (start_time,) in rows:
            # start_time lấy từ DB ra là dạng datetime object
            t = pd.to_datetime(start_time)
            
            # Tạo dữ liệu chuẩn cho bảng Time
            time_data = (
                start_time,
                t.hour,
                t.day,
                t.weekofyear,
                t.month,
                t.year,
                t.dayofweek
            )
            
            try:
                # Insert Ignore để tránh lỗi nếu đã có
                cur.execute(time_table_insert, time_data)
                count += 1
            except mysql.connector.Error as err:
                pass # Bỏ qua nếu trùng lặp
            
            if count % 50 == 0:
                print(f"   ... Đã xử lý {count}", end="\r")

        # Bật lại kiểm tra khóa
        cur.execute("SET FOREIGN_KEY_CHECKS=1;")
        
        print(f"\n✅ Đã cập nhật xong bảng TIME.")
        
    except Exception as e:
        print(f"❌ Lỗi: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    fix_time_dimension()