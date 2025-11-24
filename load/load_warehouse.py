import os
import glob
import pandas as pd
import h5py
from db import  create_connection_Warehouse
from sql_queries import artist_table_insert, song_table_insert
from config import SONG_DATA_DIR

def _fix(v):
    """Hàm xử lý giá trị None / NaN."""
    if pd.isna(v):
        return None
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
            
            # --- Lấy dữ liệu nghệ sĩ (Artist Data) ---
            artist_name = get_h5_value(metadata_songs, 'artist_name', default_val=None)
            artist_location = get_h5_value(metadata_songs, 'artist_location', default_val=None)
            artist_longitude = get_h5_value(metadata_songs, 'artist_longitude', default_val=None)
            artist_latitude = get_h5_value(metadata_songs, 'artist_latitude', default_val=None)

            # Bỏ qua file nếu thiếu thông tin ID quan trọng
            if not song_id or not artist_id:
                # print(f"Bỏ qua file do thiếu song_id hoặc artist_id: {filepath}")
                return

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
                
                pass

    except Exception as e:
       
        print(f"Lỗi nghiêm trọng khi đọc file H5 {filepath}: {e}")

def process_all_songs(cur, conn, data_path):
    """
    Duyệt toàn bộ thư mục song_data để load vào warehouse.
    """
    all_files = []
    for root, dirs, files in os.walk(data_path):
        files = glob.glob(os.path.join(root, "*.h5"))
        for f in files:
            all_files.append(os.path.abspath(f))
    
    num_files = len(all_files)
    print(f"🎵 Tổng cộng {num_files} file nhạc cần load vào warehouse.")

    for i, file in enumerate(all_files, 1):
        process_song_file(cur, file)
        conn.commit()
        if i % 100 == 0 or i == num_files:
            print(f" Đã xử lý {i}/{num_files} files.")

def load_to_warehouse(cur, conn):
    """
    Load dữ liệu từ song_data (Million Song Subset) vào warehouse.
    """
    process_all_songs(cur, conn, SONG_DATA_DIR)

def main():
    cur, conn = create_connection_Warehouse()
    if not cur or not conn:
        print("Không thể kết nối tới DB. Hủy bỏ load_warehouse.")
        return
        
    print("Kết nối thành công tới MySQL!")
    load_to_warehouse(cur, conn)
    conn.close()
    print(" Load warehouse hoàn tất.")

if __name__ == "__main__":
    main()