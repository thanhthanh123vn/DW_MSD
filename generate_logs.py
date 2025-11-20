# generate_logs.py
import json
import os
import random
import time
from config import LOG_DATA_DIR

def generate_log_data():
    # Tạo thư mục nếu chưa có
    if not os.path.exists(LOG_DATA_DIR):
        os.makedirs(LOG_DATA_DIR)
        print(f"Đã tạo thư mục: {LOG_DATA_DIR}")

    print(f"=== BẮT ĐẦU TẠO LOG GIẢ LẬP TẠI: {LOG_DATA_DIR} ===")
    
    # Tạo 5 file log giả
    for i in range(1, 6):
        filename = f"2018-11-{i:02d}-events.json"
        filepath = os.path.join(LOG_DATA_DIR, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            # Mỗi file có 50 dòng log (sự kiện)
            for _ in range(50):
                # Cấu trúc khớp với process_log_file trong load_staging.py
                event = {
                    "artist": "Synthesized Artist",
                    "auth": "Logged In",
                    "firstName": "User",
                    "gender": random.choice(["M", "F"]),
                    "itemInSession": random.randint(0, 50),
                    "lastName": f"Test{random.randint(1,100)}",
                    "length": random.uniform(100, 300),
                    "level": random.choice(["free", "paid"]),
                    "location": "Vietnam",
                    "method": "PUT",
                    "page": "NextSong",  # Quan trọng: process_log_file chỉ lấy NextSong
                    "registration": 1541000000000,
                    "sessionId": random.randint(100, 999),
                    "song": "Synthesized Song",
                    "status": 200,
                    "ts": int(time.time() * 1000), # Thời gian hiện tại (ms)
                    "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                    "userId": str(random.randint(1, 20))
                }
                # Ghi dạng JSON Lines (mỗi dòng 1 json object)
                f.write(json.dumps(event) + "\n")
        
        print(f"-> Đã tạo file: {filename}")

    print("=== HOÀN TẤT TẠO DỮ LIỆU LOG ===")

if __name__ == "__main__":
    generate_log_data()