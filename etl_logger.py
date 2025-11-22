# etl_logger.py
from db import create_connection
from sql_queries import etl_log_insert, etl_log_update_success, etl_log_update_fail

class ETLLogger:
    def __init__(self, package_name):
        self.package_name = package_name
        self.log_id = None
        self.cur = None
        self.conn = None

    def start(self):
        """Bắt đầu ghi log: Trạng thái RUNNING"""
        try:
            self.cur, self.conn = create_connection()
            self.cur.execute(etl_log_insert, (self.package_name,))
            self.conn.commit()
            self.log_id = self.cur.lastrowid
            print(f"[LOG STARTED] {self.package_name} (Log ID: {self.log_id})")
        except Exception as e:
            print(f"[LOG ERROR] Không thể khởi tạo log: {e}")

    def log_success(self, extracted=0, loaded=0, rejected=0):
        """Ghi nhận thành công: Trạng thái SUCCESS"""
        if not self.log_id or not self.conn:
            return
        try:
            self.cur.execute(etl_log_update_success, (extracted, loaded, rejected, self.log_id))
            self.conn.commit()
            print(f"[LOG SUCCESS] Extracted: {extracted}, Loaded: {loaded}, Rejected: {rejected}")
        except Exception as e:
            print(f"[LOG ERROR] Lỗi khi update success: {e}")
        finally:
            self.close()

    def log_fail(self, error_message):
        """Ghi nhận thất bại: Trạng thái FAILED"""
        if not self.log_id or not self.conn:
            return
        try:
            # Cắt lỗi nếu quá dài để tránh lỗi DB
            err_str = str(error_message)[:5000]
            self.cur.execute(etl_log_update_fail, (err_str, self.log_id))
            self.conn.commit()
            print(f"[LOG FAILED] Đã ghi nhận lỗi vào DB.")
        except Exception as e:
            print(f"[LOG ERROR] Lỗi khi update fail: {e}")
        finally:
            self.close()

    def close(self):
        if self.conn:
            try:
                self.conn.close()
            except:
                pass