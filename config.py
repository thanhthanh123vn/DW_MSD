# config.py
import os

# MySQL connection (FreeSQLDatabase sample)
DB_CONFIG = {
    "host": os.getenv("DW_DB_HOST", "sql3.freesqldatabase.com"),
    "user": os.getenv("DW_DB_USER", "sql3806292"),
    "password": os.getenv("DW_DB_PASSWORD", "M5migIKcRH"),
    "database": os.getenv("DW_DB_NAME", "sql3806292"),
    "port": int(os.getenv("DW_DB_PORT", 3306)),
}

# Local staging folders
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STAGING_DATA_DIR = os.getenv("DW_STAGING_DATA_DIR", os.path.join(BASE_DIR, "data"))
# subfolders
SONG_DATA_DIR = os.path.join(STAGING_DATA_DIR, "song_data")
LOG_DATA_DIR = os.path.join(STAGING_DATA_DIR, "log_data")

# Schedules (document only — real scheduling via cron / task scheduler)
SCHEDULE = {
    "extract": "18:00",
    "load_staging": "19:00",
    "transform": "19:30",
    "load_warehouse": "20:00",
    "create_aggregate": "20:30",
    "load_mart": "20:30"
}

