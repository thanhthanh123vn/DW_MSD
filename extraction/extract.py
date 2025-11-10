
import os
import tarfile
# ROOT = os.path.dirname(os.path.abspath(__file__))  # D:\DW\Staging
from config import BASE_DIR, SONG_DATA_DIR

def extract_tar_file(tar_path, extract_to):
    """
    Giải nén file .tar vào thư mục staging (song_data).
    """
    if not os.path.exists(tar_path):
        print(f"Không tìm thấy file: {tar_path}")
        return

    os.makedirs(extract_to, exist_ok=True)

    try:
        with tarfile.open(tar_path, "r") as tar:
            tar.extractall(path=extract_to)
            print(f"Đã giải nén {tar_path} vào {extract_to}")
    except Exception as e:
        print(f"Lỗi khi giải nén: {e}")

def main():
  
    tar_path = r"D:\DW\millionsongsubset.tar.gz"

    # Thư mục đích — theo cấu trúc hệ thống
    extract_to = SONG_DATA_DIR

    print("=== BẮT ĐẦU EXTRACT DỮ LIỆU TỪ FILE TAR ===")
    extract_tar_file(tar_path, extract_to)
    print("Hoàn tất extract, dữ liệu đã sẵn sàng trong folder staging.")

if __name__ == "__main__":
    main()
