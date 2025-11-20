# pipeline_runner.py
import subprocess
import sys
import os

ROOT = os.path.dirname(os.path.abspath(__file__))

# Danh sách các bước theo đúng thứ tự chuẩn:
# 1. Tạo bảng (quan trọng nhất, nếu không sẽ lỗi "Table doesn't exist")
# 2. Extract (Giải nén data nhạc)
# 3. Load Staging (Đọc JSON log & H5 music -> Ghi vào bảng Staging: songs, artists, songplays...)
# 4. Load Warehouse (Xử lý dữ liệu gốc H5 chi tiết -> Ghi vào Warehouse)
# 5. Transform (Tổng hợp dữ liệu -> Ghi vào bảng aggregate songplays_daily)
# 6. Load Mart (Đưa dữ liệu báo cáo -> Ghi vào bảng Mart mart_daily_plays)

SCRIPTS = [
    "create_tables",          
    "extraction.extract",
    "load.load_staging",        # Script này sẽ đọc file JSON vừa tạo ở Bước 1
    "load.load_warehouse",
    "transform.create_aggregate",
    "load.load_mart"
]

def run_script(module_name):
    print(f"\n>>> Running {module_name}...")
    
    # Thiết lập môi trường UTF-8 để tránh lỗi font chữ/ký tự lạ
    child_env = os.environ.copy()
    child_env["PYTHONUTF8"] = "1"

    # Chạy script con
    res = subprocess.run(
        [sys.executable, "-m", module_name], 
        cwd=ROOT,
        capture_output=True, 
        env=child_env
    )
    
    # In kết quả ra màn hình
    if res.stdout:
        print(res.stdout.decode('utf-8', errors='ignore'))
    
    # Báo lỗi nếu có
    if res.returncode != 0:
        print(f"!!! ERROR in {module_name} (stderr):")
        if res.stderr:
            print(res.stderr.decode('utf-8', errors='ignore'))
        # Dừng pipeline nếu gặp lỗi
        raise SystemExit(res.returncode)

def main():
    for s in SCRIPTS:
        # Kiểm tra file tồn tại trước khi chạy (đường dẫn xử lý dấu chấm thành dấu gạch chéo)
        # Lưu ý: create_tables là file gốc, các file khác nằm trong thư mục con
        if "." in s:
            script_path = os.path.join(ROOT, *s.split('.')) + ".py"
        else:
            script_path = os.path.join(ROOT, s + ".py")

        if not os.path.exists(script_path):
            print(f"Cảnh báo: Không tìm thấy file {script_path}, bỏ qua bước này.")
            continue
            
        try:
            run_script(s)
        except SystemExit:
            print("Pipeline đã dừng lại do lỗi.")
            break
    print("\n=== PIPELINE HOÀN TẤT ===")

if __name__ == "__main__":
    main()