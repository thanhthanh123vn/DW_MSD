# scripts/load/load_staging.py
import os
import glob
import uuid
import pandas as pd
import h5py
import sys

# --- CẤU HÌNH ĐƯỜNG DẪN ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# !!! QUAN TRỌNG: Kết nối tới Warehouse để nạp dữ liệu !!!
from db import create_connection_Warehouse 
from sql_queries import (
    song_table_insert,
    artist_table_insert,
    time_table_insert,
    user_table_insert,
    songplay_table_insert,
    song_select
)
from config import SONG_DATA_DIR, LOG_DATA_DIR
from etl_logger import ETLLogger

# Biến toàn cục thống kê
STATS = {"extracted": 0, "loaded": 0, "rejected": 0}

def _fix(v):
    """Xử lý giá trị NaN/None"""
    try:
        return None if pd.isna(v) else v
    except:
        return v

def get_h5_value(h5_group, field_name, index=0, default_val=None):
    """Lấy giá trị an toàn từ file H5"""
    try:
        value = h5_group[field_name][index]
        if isinstance(value, bytes):
            return value.decode('utf-8')
        if hasattr(value, 'item'):
            return value.item()
        return value
    except (KeyError, IndexError, TypeError, ValueError):
        return default_val

def safe_float(value):
    """Chuyển đổi float an toàn, tránh lỗi crash khi gặp NaN"""
    try:
        val = _fix(value)
        if val is None: return None
        return float(val)
    except:
        return None

def process_song_file(cur, filepath):
    """Xử lý 1 file nhạc H5"""
    global STATS
    STATS["extracted"] += 1
    
    try:
        with h5py.File(filepath, 'r') as f:
            metadata_songs = f.get('metadata', {}).get('songs', {})
            analysis_songs = f.get('analysis', {}).get('songs', {})

            song_id = get_h5_value(metadata_songs, 'song_id')
            artist_id = get_h5_value(metadata_songs, 'artist_id')
            
            if not song_id or not artist_id:
                STATS["rejected"] += 1
                return 

            # --- 1. NẠP ARTIST TRƯỚC (Bảng Cha) ---
            # Phải nạp Artist trước để tạo Khóa ngoại cho Song
            artist_data = (
                _fix(artist_id), 
                get_h5_value(metadata_songs, 'artist_name'), 
                get_h5_value(metadata_songs, 'artist_location'),
                safe_float(get_h5_value(metadata_songs, 'artist_longitude')),
                safe_float(get_h5_value(metadata_songs, 'artist_latitude')),
            )
            cur.execute(artist_table_insert, artist_data)

            # --- 2. NẠP SONG SAU (Bảng Con) ---
            song_data = (
                _fix(song_id), 
                get_h5_value(metadata_songs, 'title'), 
                _fix(artist_id),
                int(_fix(get_h5_value(metadata_songs, 'year', 0) or 0)),
                safe_float(get_h5_value(analysis_songs, 'duration')),
            )
            cur.execute(song_table_insert, song_data)
            
            STATS["loaded"] += 1

    except Exception as e:
        # print(f"Lỗi file H5 {filepath}: {e}")
        STATS["rejected"] += 1

def process_log_file(cur, filepath):
    """Xử lý 1 file Log JSON"""
    global STATS
    try:
        df = pd.read_json(filepath, lines=True)
        if "page" in df.columns:
            df = df[df["page"] == "NextSong"]
        
        if df.empty: return

        STATS["extracted"] += len(df)

        # 1. Process Time
        if "ts" in df.columns:
            # Convert to datetime objects
            for i, row in df.iterrows():
                t = pd.to_datetime(row['ts'], unit='ms')
                time_vals = (t, t.hour, t.day, t.isocalendar().week, t.month, t.year, t.weekday())
                try: cur.execute(time_table_insert, time_vals)
                except: pass

        # 2. Process Users
        if "userId" in df.columns:
            for _, row in df.iterrows():
                if str(row.userId).isdigit():
                    user_vals = (int(row.userId), row.firstName, row.lastName, row.gender, row.level)
                    try: cur.execute(user_table_insert, user_vals)
                    except: pass

        # 3. Process Songplays
        for _, row in df.iterrows():
            songid, artistid = None, None
            
            # Thử tìm bài hát khớp (Logic này thường trả về None với log giả)
            if row.song:
                try:
                    cur.execute(song_select, (row.song, row.artist, row.length))
                    result = cur.fetchone()
                    if result: songid, artistid = result
                except: pass

            songplay_data = (
                str(uuid.uuid4()),
                pd.to_datetime(row.ts, unit="ms"),
                int(row.userId) if str(row.userId).isdigit() else None,
                row.level,
                songid, 
                artistid,
                row.sessionId,
                row.location,
                row.userAgent
            )
            try:
                cur.execute(songplay_table_insert, songplay_data)
                STATS["loaded"] += 1
            except Exception:
                STATS["rejected"] += 1

    except Exception as e:
        print(f"Lỗi đọc file log {filepath}: {e}")
        STATS["rejected"] += 1

def process_data(cur, conn, filepath, func, file_extension):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, file_extension))
        for f in files:
            all_files.append(os.path.abspath(f))
    
    num_files = len(all_files)
    print(f"Đang xử lý {num_files} file trong {filepath} ({file_extension})")

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit() 
        if i % 100 == 0:
            print(f" -> Đã xong {i}/{num_files}")

def main():
    logger = ETLLogger("load.load_staging")
    logger.start()
    
    # KẾT NỐI TỚI WAREHOUSE
    cur, conn = create_connection_Warehouse()
    
    try:
        # Load songs
        process_data(cur, conn, SONG_DATA_DIR, process_song_file, "*.h5")
        # Load logs
        process_data(cur, conn, LOG_DATA_DIR, process_log_file, "*.json")
        
        logger.log_success(STATS["extracted"], STATS["loaded"], STATS["rejected"])
        print("=== Load Staging (vào Warehouse) Hoàn tất ===")
        
    except Exception as e:
        print(f"Lỗi Critical: {e}")
        logger.log_fail(str(e))
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    main()