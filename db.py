# db.py
import mysql.connector
from mysql.connector import Error
from config import DB_COMMON, DB_NAMES

def _get_connection(db_name_key):
    """Hàm nội bộ để tạo kết nối dựa trên key"""
    try:
        conn = mysql.connector.connect(
            host=DB_COMMON["host"],
            user=DB_COMMON["user"],
            password=DB_COMMON["password"],
            port=DB_COMMON["port"],
            database=DB_NAMES[db_name_key], # Lấy tên DB từ config
            autocommit=False
        )
        cur = conn.cursor(buffered=True)
        return cur, conn
    except Error as e:
        print(f"Error connecting to {DB_NAMES[db_name_key]}: {e}")
        raise

def create_connection_Staging():
    return _get_connection("staging")

def create_connection_Warehouse():
    return _get_connection("warehouse")

def create_connection_Control():
    return _get_connection("control")

def create_connection_Mart():
    return _get_connection("mart")