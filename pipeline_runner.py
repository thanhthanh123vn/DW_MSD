import subprocess
import sys
import os

ROOT = os.path.dirname(os.path.abspath(__file__))  # D:\DW\Staging

# Sửa thành TÊN MODULE (dấu chấm)
SCRIPTS = [
    "extraction.extract",
    "load.load_staging",
    "load.load_warehouse",
    "transform.create_aggregate",
    "load.load_mart"
]

def run_script(module_name):
    print(f"\nRunning {module_name}\n")
    
    # --- BẮT ĐẦU THAY ĐỔI QUAN TRỌNG ---
    
    # 1. Lấy một bản sao của môi trường hiện tại
    child_env = os.environ.copy()
    
    # 2. Đặt biến PYTHONUTF8=1. 
    #    Biến này "ép" trình thông dịch Python của script con
    #    phải sử dụng UTF-8 cho mọi thứ (stdout, stderr, etc.)
    child_env["PYTHONUTF8"] = "1"

    # 3. Chạy script con với môi trường đã được "ép" UTF-8
    res = subprocess.run(
        [sys.executable, "-m", module_name], 
        cwd=ROOT,
        capture_output=True, # Vẫn bắt output dạng bytes
        env=child_env        # <-- Sử dụng môi trường đã sửa
    )
    
    # --- KẾT THÚC THAY ĐỔI ---
    
    # 4. Tự giải mã output (dạng bytes) sang string (utf-8)
    #    và "ignore" (lờ đi) bất kỳ byte rác nào (như lỗi 0xd0)
    if res.stdout:
        print(res.stdout.decode('utf-8', errors='ignore'))
    
    # Nếu script chạy bị lỗi (returncode != 0)
    if res.returncode != 0:
        print("ERROR (stderr):")
        # Tự giải mã lỗi (stderr) và bỏ qua lỗi
        if res.stderr:
            print(res.stderr.decode('utf-8', errors='ignore'))
        raise SystemExit(res.returncode)

def main():
    for s in SCRIPTS:
        script_path = os.path.join(ROOT, *s.split('.')) + ".py"
        
        if not os.path.exists(script_path):
            print(f"Không tìm thấy script: {script_path} — Kiểm tra lại đường dẫn.")
            raise SystemExit(1)
            
        try:
            run_script(s)
        except SystemExit as e:
            print("Pipeline stopped due to error.")
            break

if __name__ == "__main__":
    main()