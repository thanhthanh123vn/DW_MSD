import os, glob, uuid
import pandas as pd
import h5py
from db import create_connection
from sql_queries import song_table_insert, artist_table_insert, time_table_insert, user_table_insert, songplay_table_insert, song_select
from config import SONG_DATA_DIR, LOG_DATA_DIR

def _fix(v):
    """Hàm xử lý giá trị None / NaN."""
    import pandas as pd
    try:
        return None if pd.isna(v) else v
    except Exception:
        return v

def get_h5_value(h5_group, field_name, index=0, default_val=None):
    """
    Trích xuất an toàn một giá trị từ nhóm H5,
    xử lý các trường bị thiếu và giải mã bytes.
    """
    try:
        value = h5_group[field_name][index]
        if isinstance(value, bytes):
            return value.decode('utf-8')
        # Chuyển đổi các kiểu dữ liệu của numpy (như np.float64) 
        # sang kiểu gốc của Python (float)
        if hasattr(value, 'item'):
            return value.item()
        return value
    except (KeyError, IndexError, TypeError, ValueError):
        # Trả về giá trị mặc định nếu không tìm thấy trường
        return default_val

def process_song_file(cur, filepath):
    """
    Đọc file HDF5 (H5) và insert dữ liệu.
    Phiên bản này xử lý các file bị thiếu trường (field) một cách an toàn.
    """
    try:
        with h5py.File(filepath, 'r') as f:
            # Trỏ tới các group chính một cách an toàn
            metadata_songs = f.get('metadata', {}).get('songs', {})
            analysis_songs = f.get('analysis', {}).get('songs', {})

            # --- Lấy dữ liệu bài hát (Song Data) ---
            song_id = get_h5_value(metadata_songs, 'song_id', default_val=None)
            title = get_h5_value(metadata_songs, 'title', default_val=None)
            artist_id = get_h5_value(metadata_songs, 'artist_id', default_val=None)
            # Đây là trường bị lỗi, đặt default_val=0
            year = get_h5_value(metadata_songs, 'year', default_val=0) 
            duration = get_h5_value(analysis_songs, 'duration', default_val=None)
            
            # Bỏ qua file nếu thiếu thông tin ID quan trọng
            if not song_id or not artist_id:
                # print(f"Bỏ qua file do thiếu song_id hoặc artist_id: {filepath}")
                return 

            # --- Chèn Song Data ---
            song_data = (
                _fix(song_id),
                _fix(title),
                _fix(artist_id),
                int(_fix(year)) if _fix(year) != 0 and _fix(year) is not None else None,
                float(_fix(duration)) if _fix(duration) is not None else None,
            )
            try:
                cur.execute(song_table_insert, song_data)
            except Exception:
                 # Bỏ qua lỗi (ví dụ: trùng lặp PRIMARY KEY)
                 pass

            # --- Lấy dữ liệu nghệ sĩ (Artist Data) ---
            artist_name = get_h5_value(metadata_songs, 'artist_name', default_val=None)
            artist_location = get_h5_value(metadata_songs, 'artist_location', default_val=None)
            artist_longitude = get_h5_value(metadata_songs, 'artist_longitude', default_val=None)
            artist_latitude = get_h5_value(metadata_songs, 'artist_latitude', default_val=None)

            # --- Chèn Artist Data ---
            artist_data = (
                _fix(artist_id),
                _fix(artist_name),
                _fix(artist_location),
                float(_fix(artist_longitude)) if _fix(artist_longitude) is not None else None,
                float(_fix(artist_latitude)) if _fix(artist_latitude) is not None else None,
            )
            try:
                cur.execute(artist_table_insert, artist_data)
            except Exception:
                 # Bỏ qua lỗi (ví dụ: trùng lặp PRIMARY KEY)
                 pass

    except Exception as e:
        # Lỗi nghiêm trọng (vd: file h5 bị hỏng)
        print(f"Lỗi nghiêm trọng khi đọc file H5 {filepath}: {e}")

def process_log_file(cur, filepath):
    """
    Xử lý file log (JSON). Phần này giữ nguyên, không thay đổi.
    """
    try:
        df = pd.read_json(filepath, lines=True)
    except pd.errors.EmptyDataError:
        print(f"Bỏ qua file log rỗng: {filepath}")
        return
        
    df = df[df["page"] == "NextSong"]
    if df.empty:
        return

    t = pd.to_datetime(df["ts"], unit="ms")
    time_df = pd.DataFrame({
        "start_time": t,
        "hour": t.dt.hour,
        "day": t.dt.day,
        "week": t.dt.isocalendar().week,
        "month": t.dt.month,
        "year": t.dt.year,
        "weekday": t.dt.weekday
    })
    for _, row in time_df.iterrows():
        try:
            values = [None if pd.isna(x) else x for x in list(row)]
            cur.execute(time_table_insert, values)
        except Exception:
            pass

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

    for _, row in df.iterrows():
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
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except Exception:
            pass


def process_data(cur, conn, filepath, func, file_extension="*.h5"):
    """
    Hàm này được sửa lại một chút để nhận `file_extension`
    để có thể tìm *.h5 hoặc *.json
    """
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
    from db import create_connection
    cur, conn = create_connection()
    
    # load song data (tìm file .h5)
    process_data(cur, conn, filepath=SONG_DATA_DIR, func=process_song_file, file_extension="*.h5")
    
    # load log data (tìm file .json)
    process_data(cur, conn, filepath=LOG_DATA_DIR, func=process_log_file, file_extension="*.json")
    
    conn.close()
    print("Load staging done.")

if __name__ == "__main__":
    main()