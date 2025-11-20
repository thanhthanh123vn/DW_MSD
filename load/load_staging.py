# scripts/load/load_staging.py
import os
import glob
import uuid
import pandas as pd
import h5py
import sys

# --- CẤU HÌNH ĐƯỜNG DẪN ĐỂ IMPORT MODULE TỪ THƯ MỤC GỐC ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from db import create_connection
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
STATS = {
    "extracted": 0,
    "loaded": 0,
    "rejected": 0
}

def _fix(v):
    """Hàm xử lý giá trị None / NaN."""
    try:
        return None if pd.isna(v) else v
    except Exception:
        return v

def get_h5_value(h5_group, field_name, index=0, default_val=None):
    try:
        value = h5_group[field_name][index]
        if isinstance(value, bytes):
            return value.decode('utf-8')
        if hasattr(value, 'item'):
            return value.item()
        return value
    except (KeyError, IndexError, TypeError, ValueError):
        return default_val

def process_song_file(cur, filepath):
    """Xử lý 1 file nhạc H5"""
    global STATS
    STATS["extracted"] += 1
    
    try:
        with h5py.File(filepath, 'r') as f:
            metadata_songs = f.get('metadata', {}).get('songs', {})
            analysis_songs = f.get('analysis', {}).get('songs', {})

            # --- Song Data ---
            song_id = get_h5_value(metadata_songs, 'song_id', default_val=None)
            title = get_h5_value(metadata_songs, 'title', default_val=None)
            artist_id = get_h5_value(metadata_songs, 'artist_id', default_val=None)
            year = get_h5_value(metadata_songs, 'year', default_val=0) 
            duration = get_h5_value(analysis_songs, 'duration', default_val=None)
            
            if not song_id or not artist_id:
                STATS["rejected"] += 1
                return 

            song_data = (
                _fix(song_id), _fix(title), _fix(artist_id),
                int(_fix(year)) if _fix(year) != 0 and _fix(year) is not None else None,
                float(_fix(duration)) if _fix(duration) is not None else None,
            )
            try:
                cur.execute(song_table_insert, song_data)
                STATS["loaded"] += 1
            except Exception:
                STATS["rejected"] += 1
                pass

            # --- Artist Data ---
            artist_name = get_h5_value(metadata_songs, 'artist_name', default_val=None)
            artist_location = get_h5_value(metadata_songs, 'artist_location', default_val=None)
            artist_longitude = get_h5_value(metadata_songs, 'artist_longitude', default_val=None)
            artist_latitude = get_h5_value(metadata_songs, 'artist_latitude', default_val=None)

            artist_data = (
                _fix(artist_id), _fix(artist_name), _fix(artist_location),
                float(_fix(artist_longitude)) if _fix(artist_longitude) is not None else None,
                float(_fix(artist_latitude)) if _fix(artist_latitude) is not None else None,
            )
            try:
                cur.execute(artist_table_insert, artist_data)
                STATS["loaded"] += 1
            except Exception:
                pass

    except Exception as e:
        # print(f"Lỗi file H5: {filepath} - {e}") # Bỏ comment nếu muốn xem chi tiết
        STATS["rejected"] += 1

def process_log_file(cur, filepath):
    """Xử lý 1 file Log JSON"""
    global STATS
    try:
        # --- PHẦN QUAN TRỌNG NHẤT: BẮT LỖI JSON ---
        df = pd.read_json(filepath, lines=True)
    except (ValueError, pd.errors.EmptyDataError, pd.errors.ParserError) as e:
        # Bắt tất cả lỗi liên quan đến định dạng file (ValueError chính là lỗi No ':' found)
        print(f"⚠️ Bỏ qua file log lỗi: {os.path.basename(filepath)} | Lỗi: {e}")
        STATS["rejected"] += 1
        return
    except Exception as e:
        print(f"⚠️ Lỗi không xác định khi đọc file {filepath}: {e}")
        STATS["rejected"] += 1
        return
    # --------------------------------------------
        
    # Chỉ lấy log nghe nhạc
    if "page" in df.columns:
        df = df[df["page"] == "NextSong"]
    else:
        return

    if df.empty:
        return

    STATS["extracted"] += len(df)

    # 1. Process Time
    if "ts" in df.columns:
        t = pd.to_datetime(df["ts"], unit="ms")
        time_df = pd.DataFrame({
            "start_time": t, "hour": t.dt.hour, "day": t.dt.day, "week": t.dt.isocalendar().week,
            "month": t.dt.month, "year": t.dt.year, "weekday": t.dt.weekday
        })
        for _, row in time_df.iterrows():
            try:
                values = [None if pd.isna(x) else x for x in list(row)]
                cur.execute(time_table_insert, values)
            except Exception:
                pass

    # 2. Process Users
    if "userId" in df.columns:
        user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
        for _, row in user_df.iterrows():
            if str(row.userId).isdigit():
                try:
                    vals = (
                        int(row.userId),
                        None if pd.isna(row.firstName) else row.firstName,
                        None if pd.isna(row.lastName) else row.lastName,
                        None if pd.isna(row.gender) else row.gender,
                        None if pd.isna(row.level) else row.level,
                    )
                    cur.execute(user_table_insert, vals)
                except Exception:
                    pass

    # 3. Process Songplays
    for _, row in df.iterrows():
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
            result = cur.fetchone()
            songid, artistid = result if result else (None, None)
            
            start_time = pd.to_datetime(row.ts, unit="ms")
            song_play_id = str(uuid.uuid4())
            
            songplay_data = (
                song_play_id,
                start_time.to_pydatetime() if hasattr(start_time, 'to_pydatetime') else start_time,
                int(row.userId) if str(row.userId).isdigit() else None,
                None if pd.isna(row.level) else row.level,
                None if pd.isna(songid) else songid,
                None if pd.isna(artistid) else artistid,
                None if pd.isna(row.sessionId) else row.sessionId,
                None if pd.isna(row.location) else row.location,
                None if pd.isna(row.userAgent) else row.userAgent,
            )
            cur.execute(songplay_table_insert, songplay_data)
            STATS["loaded"] += 1 
        except Exception:
            STATS["rejected"] += 1
            pass

def process_data(cur, conn, filepath, func, file_extension="*.h5"):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, file_extension))
        for f in files:
            all_files.append(os.path.abspath(f))
            
    num_files = len(all_files)
    print(f"{num_files} files found in {filepath} (Loại: {file_extension})")

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit() 
        if i % 100 == 0 or i == num_files:
            print(f"{i}/{num_files} processed: {datafile}")

def main():
    logger = ETLLogger("load.load_staging")
    logger.start()
    
    cur, conn = create_connection()
    
    try:
        # Load song data
        process_data(cur, conn, filepath=SONG_DATA_DIR, func=process_song_file, file_extension="*.h5")
        
        # Load log data
        process_data(cur, conn, filepath=LOG_DATA_DIR, func=process_log_file, file_extension="*.json")
        
        logger.log_success(
            extracted=STATS["extracted"], 
            loaded=STATS["loaded"], 
            rejected=STATS["rejected"]
        )
        print("Load staging done.")
        
    except Exception as e:
        print(f"Critical Error: {e}")
        logger.log_fail(str(e))
        # Không raise lỗi nữa để pipeline chạy tiếp các bước sau
        # raise 
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()