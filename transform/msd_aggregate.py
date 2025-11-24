# transform/export_mart.py
import os
import sys
import pandas as pd

# --- Cấu hình đường dẫn để import các module ở thư mục cha ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from db import create_connection_Mart
from config import STAGING_DATA_DIR
from etl_logger import ETLLogger

def export_mart_data():
    """
    Xuất dữ liệu báo cáo từ Database Mart ra các file CSV.
    """
    # 1. Khởi tạo Logger
    logger = ETLLogger("transform.export_mart")
    logger.start()

    cur = None
    conn = None
    
    # Danh sách các bảng cần export
    tables_to_export = [
        "top_artists",
        "songplays_daily",
        "system_overview"
    ]
    
    try:
        print("\n=== BẮT ĐẦU EXPORT DATA MART ===")
        
        # 2. Kết nối Database Mart
        cur, conn = create_connection_Mart()
        
        # Tạo thư mục output nếu chưa có
        export_dir = os.path.join(STAGING_DATA_DIR, "export")
        os.makedirs(export_dir, exist_ok=True)
        
        total_rows = 0
        
        for table in tables_to_export:
            print(f"-> Đang xử lý bảng: {table}...")
            
            # 3. Truy vấn dữ liệu
            query = f"SELECT * FROM {table}"
            cur.execute(query)
            
            # Lấy data và tên cột
            rows = cur.fetchall()
            if cur.description:
                columns = [col[0] for col in cur.description]
                df = pd.DataFrame(rows, columns=columns)
            else:
                df = pd.DataFrame()

            # 4. Xuất ra CSV
            file_name = f"mart_{table}.csv"
            output_file = os.path.join(export_dir, file_name)
            
            df.to_csv(output_file, index=False, encoding='utf-8')
            
            row_count = len(df)
            total_rows += row_count
            print(f"   ✅ Đã xuất {row_count} dòng ra: {file_name}")

        # 5. Ghi log tổng kết
        logger.log_success(extracted=total_rows, loaded=total_rows, rejected=0)
        print(f"\n=== HOÀN TẤT. Tổng cộng {total_rows} dòng dữ liệu đã được export. ===")
        
    except Exception as e:
        print(f"❌ Lỗi khi export mart: {e}")
        logger.log_fail(str(e))
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    export_mart_data()