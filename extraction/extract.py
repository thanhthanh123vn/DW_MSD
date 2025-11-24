# extraction/extract.py
import os
import sys
import tarfile
import shutil

# --- CẤU HÌNH ĐƯỜNG DẪN ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
# ---------------------------

from config import SONG_DATA_DIR
from etl_logger import ETLLogger

def main():
    logger = ETLLogger("extraction.extract")
    logger.start()

    tar_path = r"D:\DW\millionsongsubset.tar.gz" 
    extract_to = SONG_DATA_DIR
    
    try:
        print("=== BẮT ĐẦU EXTRACT ===") 
        
        if not os.path.exists(tar_path):
            raise FileNotFoundError(f"Không tìm thấy file nén: {tar_path}")

        # --- KIỂM TRA THÔNG MINH: Nếu đã có dữ liệu thì bỏ qua ---
        # Kiểm tra xem thư mục đích đã có dữ liệu con chưa (ví dụ kiểm tra folder 'MillionSongSubset')
        check_path = os.path.join(extract_to, "MillionSongSubset")
        
        if os.path.exists(check_path):
            print(f"⚠️ Phát hiện dữ liệu đã tồn tại tại: {extract_to}")
            print("-> Bỏ qua bước giải nén để tránh lỗi và tiết kiệm thời gian.")
            print("(Nếu bạn muốn giải nén lại, hãy xóa thủ công thư mục 'data/song_data' trước)")
            
            # Ghi log thành công giả định (vì dữ liệu đã sẵn sàng)
            logger.log_success(extracted=0, loaded=0, rejected=0)
            return
        # ----------------------------------------------------------

        os.makedirs(extract_to, exist_ok=True)
        
        print("-> Đang giải nén... (Vui lòng đợi)")
        file_count = 0
        with tarfile.open(tar_path, "r") as tar:
            # Đếm số lượng file
            members = tar.getmembers()
            file_count = len(members)
            
            # Giải nén
            # filter='data' giúp tránh cảnh báo DeprecationWarning trên Python mới, 
            # nhưng để tương thích an toàn ta dùng try-except hoặc mặc định
            try:
                tar.extractall(path=extract_to, filter='data')
            except TypeError:
                # Fallback cho python phiên bản cũ hơn không hỗ trợ filter
                tar.extractall(path=extract_to)
                
            print(f"✅ Đã giải nén xong: {tar_path}")

        logger.log_success(extracted=file_count, loaded=0, rejected=0)

    except Exception as e:
        print(f"❌ Lỗi: {e}")
        logger.log_fail(e)
        raise 

if __name__ == "__main__":
    main()