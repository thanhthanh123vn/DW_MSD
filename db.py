# db.py
import mysql.connector
from mysql.connector import Error
from config import DB_CONFIG

def create_connection():
    """Trả về (cursor, conn) giống mã cũ."""
    try:
        conn = mysql.connector.connect(
            host=DB_CONFIG["host"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
            port=DB_CONFIG.get("port", 3306),
            autocommit=False
        )
        cur = conn.cursor(buffered=True)  # buffered để fetchone() an toàn
        print("Kết nối thành công tới MySQL!")
        return cur, conn
    except Error as e:
        print(f"Lỗi kết nối DB: {e}")
        raise
