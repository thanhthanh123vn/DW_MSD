# extraction/extract.py
import os
import tarfile
from config import SONG_DATA_DIR
import sys

# Thêm đường dẫn để import được etl_logger từ thư mục cha
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) 
from etl_logger import ETLLogger

def main():
    # Khởi tạo logger
    logger = ETLLogger("extraction.extract")
    logger.start()

    # Đường dẫn file nén (Đảm bảo đường dẫn này đúng trên máy bạn)
    tar_path = r"D:\DW\millionsongsubset.tar.gz" 
    extract_to = SONG_DATA_DIR
    
    try:
        # --- ĐÃ SỬA: Dùng tiếng Việt không dấu hoặc tiếng Anh ---
        print("=== BAT DAU EXTRACT (Starting Extract) ===") 
        
        if not os.path.exists(tar_path):
            raise FileNotFoundError(f"File not found: {tar_path}")

        os.makedirs(extract_to, exist_ok=True)
        
        file_count = 0
        with tarfile.open(tar_path, "r") as tar:
            # Đếm số file sẽ giải nén
            members = tar.getmembers()
            file_count = len(members)
            tar.extractall(path=extract_to)
            print(f"Da giai nen xong: {tar_path}")

        # Ghi log thành công
        logger.log_success(extracted=file_count, loaded=0, rejected=0)

    except Exception as e:
        # --- ĐÃ SỬA: In lỗi không dấu ---
        print(f"Error: {e}")
        logger.log_fail(e)
        raise 

if __name__ == "__main__":
    main()