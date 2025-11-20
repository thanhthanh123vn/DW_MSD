import os
import tarfile
from config import SONG_DATA_DIR
# Import Logger
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # Fix đường dẫn import
from etl_logger import ETLLogger

def main():
    # Khởi tạo logger
    logger = ETLLogger("extraction.extract")
    logger.start()

    tar_path = r"D:\DW\millionsongsubset.tar.gz"
    extract_to = SONG_DATA_DIR
    
    try:
        print("=== BẮT ĐẦU EXTRACT ===")
        if not os.path.exists(tar_path):
            raise FileNotFoundError(f"Không tìm thấy file: {tar_path}")

        os.makedirs(extract_to, exist_ok=True)
        
        file_count = 0
        with tarfile.open(tar_path, "r") as tar:
            # Đếm số file sẽ giải nén
            file_count = len(tar.getmembers())
            tar.extractall(path=extract_to)
            print(f"Đã giải nén {tar_path}")

        # Ghi log thành công (extracted = số file, loaded = 0 vì chưa load vào DB)
        logger.log_success(extracted=file_count, loaded=0, rejected=0)

    except Exception as e:
        print(f"Lỗi: {e}")
        # Ghi log lỗi
        logger.log_fail(e)
        raise # Vẫn raise lỗi để pipeline runner biết

if __name__ == "__main__":
    main()