# create_tables.py
from db import create_connection_Control, create_connection_Warehouse, create_connection_Mart
from sql_queries import create_control_queries, create_warehouse_queries, create_mart_queries

def run_queries(cur, conn, queries, db_name):
    """Hàm hỗ trợ chạy danh sách query"""
    print(f"--- Đang tạo bảng cho {db_name} ---")
    for q in queries:
        try:
            cur.execute(q)
            conn.commit()
            # In ra tên bảng vừa tạo (lấy từ sau chữ TABLE IF NOT EXISTS)
            # tableName = q.split("TABLE IF NOT EXISTS")[1].split("(")[0].strip()
            print(f" -> Đã thực thi query tạo bảng.")
        except Exception as e:
            print(f" -> Lỗi: {e}")

def main():
    # 1. Tạo bảng Log trong CONTROL DB
    try:
        cur, conn = create_connection_Control()
        run_queries(cur, conn, create_control_queries, "CONTROL DB")
        conn.close()
    except Exception as e:
        print(f"Lỗi kết nối Control: {e}")

    # 2. Tạo bảng Star Schema trong WAREHOUSE DB
    try:
        cur, conn = create_connection_Warehouse()
        # Drop cũ nếu cần (cẩn thận mất dữ liệu): 
        # cur.execute("DROP TABLE IF EXISTS songplays") ...
        run_queries(cur, conn, create_warehouse_queries, "WAREHOUSE DB")
        conn.close()
    except Exception as e:
        print(f"Lỗi kết nối Warehouse: {e}")

    # 3. Tạo bảng Báo cáo trong MART DB
    try:
        cur, conn = create_connection_Mart()
        run_queries(cur, conn, create_mart_queries, "MART DB")
        conn.close()
    except Exception as e:
        print(f"Lỗi kết nối Mart: {e}")

    print("\n=== HOÀN TẤT QUÁ TRÌNH TẠO BẢNG ===")

if __name__ == "__main__":
    main()