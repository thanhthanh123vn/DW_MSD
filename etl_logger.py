# etl_logger.py
from db import create_connection
from sql_queries import etl_log_insert, etl_log_update_success, etl_log_update_fail
import traceback

class ETLLogger:
    def __init__(self, package_name):
        self.package_name = package_name
        self.log_id = None
        self.cur = None
        self.conn = None

    def start(self):
        """Ghi nhận bắt đầu chạy script"""
        self.cur, self.conn = create_connection()
        try:
            self.cur.execute(etl_log_insert, (self.package_name,))
            self.conn.commit()
            self.log_id = self.cur.lastrowid # Lấy ID vừa tạo
            print(f"[LOG] Started logging for {self.package_name} (ID: {self.log_id})")
        except Exception as e:
            print(f"[LOG ERROR] Could not start log: {e}")

    def log_success(self, extracted=0, loaded=0, rejected=0):
        """Ghi nhận chạy thành công"""
        if not self.log_id: return
        try:
            self.cur.execute(etl_log_update_success, (extracted, loaded, rejected, self.log_id))
            self.conn.commit()
            print(f"[LOG] Finished successfully. Loaded: {loaded}")
        except Exception as e:
            print(f"[LOG ERROR] Could not update success log: {e}")
        finally:
            self.close()

    def log_fail(self, error_msg):
        """Ghi nhận lỗi"""
        if not self.log_id: return
        try:
            # Cắt chuỗi lỗi nếu quá dài
            str_error = str(error_msg)[:5000]
            self.cur.execute(etl_log_update_fail, (str_error, self.log_id))
            self.conn.commit()
            print(f"[LOG] Marked as FAILED.")
        except Exception as e:
            print(f"[LOG ERROR] Could not update failure log: {e}")
        finally:
            self.close()

    def close(self):
        if self.conn:
            self.conn.close()