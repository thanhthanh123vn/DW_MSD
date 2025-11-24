# config.py
import os

# Thông tin kết nối chung (Host, User, Pass)
DB_COMMON = {
    "host": os.getenv("DW_DB_HOST", "localhost"),
    "user": os.getenv("DW_DB_USER", "root"),
    "password": os.getenv("DW_DB_PASSWORD", ""),
    "port": int(os.getenv("DW_DB_PORT", 3306)),
}

# Tên cụ thể của 4 Database
DB_NAMES = {
    "staging": "database_staging",
    "warehouse": "database_warehouse",
    "control": "database_control",
    "mart": "database_mart"
}

# Đường dẫn thư mục (Giữ nguyên như cũ)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STAGING_DATA_DIR = os.getenv("DW_STAGING_DATA_DIR", os.path.join(BASE_DIR, "data"))
SONG_DATA_DIR = os.path.join(STAGING_DATA_DIR, "song_data")
LOG_DATA_DIR = os.path.join(STAGING_DATA_DIR, "log_data")

SCHEDULE = {
    "extract": "18:00",
    "load_staging": "19:00",
    "transform": "19:30",
    "load_warehouse": "20:00",
    "create_aggregate": "20:30",
    "load_mart": "20:30"
}

