# fix_data_warehouse.py
import mysql.connector
import random
import sys
import os

# Cấu hình import
from config import DB_COMMON, DB_NAMES

def create_conn(db_key):
    return mysql.connector.connect(
        host=DB_COMMON["host"],
        user=DB_COMMON["user"],
        password=DB_COMMON["password"],
        port=DB_COMMON["port"],
        database=DB_NAMES[db_key],
        autocommit=True
    )

def fix_data():
    print("--- BẮT ĐẦU ĐỒNG BỘ DỮ LIỆU WAREHOUSE ---")
    
    conn_wh = None
    try:
        # Kết nối tới Warehouse DB
        conn_wh = create_conn("warehouse")
        cur_wh = conn_wh.cursor()
        
        # 1. Lấy danh sách ID bài hát và nghệ sĩ THẬT
        print("1. Đang lấy danh sách bài hát thật từ bảng 'songs'...")
        cur_wh.execute("SELECT song_id, artist_id FROM songs LIMIT 2000")
        real_songs = cur_wh.fetchall() # List of (song_id, artist_id)
        
        if not real_songs:
            print("!!! LỖI: Bảng 'songs' trống. Bạn cần chạy load_staging/load_warehouse trước.")
            return

        print(f"   -> Tìm thấy {len(real_songs)} bài hát thật.")

        # 2. Lấy danh sách Songplays đang bị NULL hoặc fake
        print("2. Đang quét bảng 'songplays'...")
        cur_wh.execute("SELECT songplay_id FROM songplays")
        all_plays = cur_wh.fetchall()
        
        if not all_plays:
            print("!!! CẢNH BÁO: Bảng 'songplays' chưa có dữ liệu log.")
            return

        print(f"   -> Tìm thấy {len(all_plays)} dòng log cần sửa.")

        # 3. Cập nhật (Update) dữ liệu
        print("3. Đang trộn dữ liệu...")
        updated_count = 0
        
        # Tắt kiểm tra khóa ngoại tạm thời để update nhanh (nếu cần)
        # cur_wh.execute("SET FOREIGN_KEY_CHECKS=0;") 
        
        for (play_id,) in all_plays:
            # Chọn bừa 1 bài hát thật
            r_song_id, r_artist_id = random.choice(real_songs)
            
            # Update dòng log đó trỏ về bài hát thật
            sql = """
            UPDATE songplays 
            SET song_id = %s, artist_id = %s
            WHERE songplay_id = %s
            """
            cur_wh.execute(sql, (r_song_id, r_artist_id, play_id))
            updated_count += 1
            
            if updated_count % 100 == 0:
                print(f"   ... Đã sửa {updated_count} dòng", end="\r")
        
        print(f"\n✅ Đã cập nhật xong {updated_count} lượt nghe.")
        
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
    finally:
        if conn_wh: conn_wh.close()

if __name__ == "__main__":
    fix_data()