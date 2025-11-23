# transform/msd_aggregate.py
import os
import sys
import pandas as pd

# --- Cấu hình để Python tìm thấy file config.py và db.py ở thư mục cha ---
current_dir = os.path.dirname(os.path.abspath(__file__)) # .../transform
parent_dir = os.path.dirname(current_dir)                # .../
sys.path.append(parent_dir)

from db import create_connection
from config import STAGING_DATA_DIR
from etl_logger import ETLLogger

def dump_aggregate_data():
    """
    Thực hiện Dump dữ liệu từ bảng aggregate (songplays_daily) ra file CSV.
    """
    # 1. Khởi tạo Logger
    logger = ETLLogger("transform.dump_aggregate")
    logger.start()

    cur, conn = create_connection()
    
    try:
        print("\n=== BẮT ĐẦU DUMP AGGREGATE FILE ===")

        # 2. Truy vấn dữ liệu đã tổng hợp từ DB
        query = "SELECT date, play_count FROM songplays_daily ORDER BY date DESC;"
        cur.execute(query)
        
        # Lấy tên cột để tạo DataFrame
        columns = [col[0] for col in cur.description]
        rows = cur.fetchall()
        
        if not rows:
            print(" Cảnh báo: Bảng songplays_daily chưa có dữ liệu.")
            logger.log_success(0, 0, 0)
            return

        df = pd.DataFrame(rows, columns=columns)
        row_count = len(df)
        print(f"-> Đã lấy {row_count} dòng từ database.")

        # 3. Thiết lập đường dẫn output
        # Tạo thư mục con 'export' trong thư mục Staging Data để chứa file dump
        export_dir = os.path.join(STAGING_DATA_DIR, "export")
        os.makedirs(export_dir, exist_ok=True)
        
        output_file = os.path.join(export_dir, "songplays_daily_dump.csv")

        # 4. Xuất file CSV (Dump)
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f" Đã xuất file thành công: {output_file}")
        
        # 5. Ghi log thành công
        # Extracted = Loaded = số dòng ghi ra file
        logger.log_success(extracted=row_count, loaded=row_count, rejected=0)
        
    except Exception as e:
        print(f"Lỗi khi dump file: {e}")
        logger.log_fail(str(e))
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    dump_aggregate_data()