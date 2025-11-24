# dashboard.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import mysql.connector
import subprocess
import sys
import os
import time
from config import DB_COMMON, DB_NAMES

# --- CẤU HÌNH TRANG ---
st.set_page_config(
    page_title="Music DW Command Center", 
    page_icon="🎛️", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CSS TÙY CHỈNH ---
st.markdown("""
<style>
    div[data-testid="metric-container"] {
        background-color: #F0F2F6;
        border: 1px solid #D6D9DE;
        padding: 15px;
        border-radius: 10px;
        color: #31333F;
    }
    .stButton>button {
        width: 100%;
        border-radius: 5px;
        height: 3em;
    }
    h1, h2, h3 { color: #1E88E5; }
</style>
""", unsafe_allow_html=True)

# --- HÀM HỆ THỐNG ---
def get_connection(db_key):
    return mysql.connector.connect(
        host=DB_COMMON["host"],
        user=DB_COMMON["user"],
        password=DB_COMMON["password"],
        port=DB_COMMON["port"],
        database=DB_NAMES[db_key]
    )

@st.cache_data(ttl=60)
def query_db(query, db_key="warehouse"):
    """Chạy query SQL và trả về DataFrame"""
    try:
        conn = get_connection(db_key)
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        return pd.DataFrame()

def run_script_utf8(script_name, description):
    """Chạy script Python với hiển thị trạng thái trực quan"""
    status_text = st.empty()
    progress_bar = st.progress(0)
    
    try:
        status_text.info(f"⏳ Đang chạy: {description}...")
        progress_bar.progress(30)
        
        base_path = os.path.dirname(os.path.abspath(__file__))
        script_path = os.path.join(base_path, script_name)
        
        # Thiết lập môi trường UTF-8
        my_env = os.environ.copy()
        my_env["PYTHONIOENCODING"] = "utf-8"
        my_env["PYTHONUTF8"] = "1"

        # Chạy script
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            encoding='utf-8',
            env=my_env
        )
        
        progress_bar.progress(100)
        time.sleep(0.5)
        progress_bar.empty()
        
        if result.returncode == 0:
            status_text.success(f"✅ {description} thành công!")
            with st.expander("Xem chi tiết log (Output)", expanded=False):
                st.code(result.stdout)
            return True
        else:
            status_text.error(f"❌ Lỗi khi chạy {script_name}")
            with st.expander("Xem chi tiết lỗi (Error)", expanded=True):
                st.code(result.stderr)
            return False

    except Exception as e:
        status_text.error(f"Lỗi hệ thống: {e}")
        return False

# --- SIDEBAR: TRUNG TÂM ĐIỀU KHIỂN ---
with st.sidebar:
    st.title("🎛️ Command Center")
    st.info("Hệ thống: Online 🟢")
    
    # 1. QUY TRÌNH ETL (TÍNH NĂNG MỚI)
    st.markdown("---")
    st.header("🚀 Quy trình ETL")
    
    col_etl1, col_etl2 = st.columns(2)
    
    # Bước 1: Extract
    if st.button("1️⃣ Extract Data", help="Giải nén dữ liệu từ file .tar.gz"):
        if run_script_utf8("extraction/extract.py", "Giải nén dữ liệu"):
            st.toast("Extract hoàn tất!", icon="📦")

    # Bước 2: Load Warehouse
    if st.button("2️⃣ Load Warehouse", help="Đọc file JSON/H5 và nạp vào Warehouse DB"):
        if run_script_utf8("load/load_staging.py", "Nạp dữ liệu vào Warehouse"):
            st.toast("Load Warehouse xong!", icon="🏭")

    # Bước 3: Fix Data
    if st.button("3️⃣ Fix Data (Sync)", help="Đồng bộ ID giữa Log và Nhạc thật"):
        if run_script_utf8("fix_data.py", "Đồng bộ dữ liệu"):
            st.toast("Dữ liệu đã khớp!", icon="🔧")
            st.cache_data.clear()

    # Bước 4: Transform
    if st.button("4️⃣ Transform", help="Tính toán bảng tổng hợp theo ngày"):
        if run_script_utf8("transform/create_aggregate.py", "Tổng hợp dữ liệu"):
            st.toast("Transform xong!", icon="📊")

    # Bước 5: Load Mart
    if st.button("5️⃣ Load Mart", help="Tạo bảng báo cáo cuối cùng"):
        if run_script_utf8("load/load_mart.py", "Cập nhật báo cáo Mart"):
            st.toast("Mart đã sẵn sàng!", icon="✅")
            st.cache_data.clear()

    st.markdown("---")
    # Xuất báo cáo
    if st.button("📤 Export All Reports", type="primary"):
        if run_script_utf8("transform/export_mart.py", "Xuất file CSV"):
            st.success("File đã lưu tại: data/export/")

    st.markdown("---")
    # Bộ lọc hiển thị
    st.subheader("🔍 Xem báo cáo theo năm")
    try:
        df_years = query_db("SELECT DISTINCT year FROM time ORDER BY year DESC", "warehouse")
        year_list = df_years['year'].tolist() if not df_years.empty else [2018]
    except:
        year_list = [2018]
    selected_year = st.selectbox("Chọn Năm:", year_list)

# --- MAIN DASHBOARD ---
st.title("🎧 Music Data Warehouse Ultimate")
st.caption(f"Dữ liệu hiển thị cho năm: {selected_year}")

# TABS
tabs = st.tabs(["📊 Tổng Quan", "🎵 Bài Hát & Nghệ Sĩ", "👥 Người Dùng", "📍 Địa Lý", "🔧 Quản Trị & Logs"])

# ==================================================
# TAB 1: TỔNG QUAN
# ==================================================
with tabs[0]:
    col1, col2, col3, col4 = st.columns(4)
    
    # Query động
    q_plays = f"SELECT COUNT(*) as c FROM songplays JOIN time ON songplays.start_time = time.start_time WHERE time.year = {selected_year}"
    total_plays = query_db(q_plays).iloc[0]['c']
    
    q_users = f"SELECT COUNT(DISTINCT user_id) as c FROM songplays JOIN time ON songplays.start_time = time.start_time WHERE time.year = {selected_year}"
    active_users = query_db(q_users).iloc[0]['c']
    
    total_songs = query_db("SELECT COUNT(*) as c FROM songs").iloc[0]['c']
    
    col1.metric("Lượt nghe (Năm này)", f"{total_plays:,}")
    col2.metric("Người dùng Active", active_users)
    col3.metric("Tổng kho nhạc", f"{total_songs:,}")
    
    # Lấy Top Trending từ Mart
    df_overview = query_db("SELECT * FROM system_overview", "mart")
    top_artist = "N/A"
    if not df_overview.empty:
        row = df_overview[df_overview['metric_name'] == 'Top Trending Artist']
        if not row.empty: top_artist = row.iloc[0]['metric_value']
    col4.metric("Nghệ sĩ Hot nhất", top_artist)
    
    st.markdown("#### 📈 Xu hướng nghe nhạc")
    q_trend = f"""
        SELECT DATE(sp.start_time) as date, COUNT(*) as plays
        FROM songplays sp
        JOIN time t ON sp.start_time = t.start_time
        WHERE t.year = {selected_year}
        GROUP BY DATE(sp.start_time)
        ORDER BY date
    """
    df_trend = query_db(q_trend)
    if not df_trend.empty:
        fig = px.area(df_trend, x='date', y='plays', color_discrete_sequence=['#00CC96'])
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu xu hướng.")

# ==================================================
# TAB 2: BÀI HÁT & NGHỆ SĨ
# ==================================================
with tabs[1]:
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("🏆 Top 10 Bài Hát")
        q_top_songs = f"""
            SELECT s.title, a.name as artist, COUNT(*) as plays
            FROM songplays sp
            JOIN songs s ON sp.song_id = s.song_id
            JOIN artists a ON sp.artist_id = a.artist_id
            JOIN time t ON sp.start_time = t.start_time
            WHERE t.year = {selected_year}
            GROUP BY s.title, a.name
            ORDER BY plays DESC
            LIMIT 10
        """
        df_songs = query_db(q_top_songs)
        if not df_songs.empty:
            st.dataframe(df_songs, use_container_width=True)
        else:
            st.warning("Chưa có dữ liệu bài hát.")

    with c2:
        st.subheader("🎤 Top 10 Nghệ Sĩ (Mart)")
        # Lấy từ Mart cho nhanh
        df_artists = query_db("SELECT artist_name, total_plays, top_song_name FROM top_artists ORDER BY total_plays DESC LIMIT 10", "mart")
        if not df_artists.empty:
            fig_bar = px.bar(df_artists, x='total_plays', y='artist_name', orientation='h', 
                             color='total_plays', text='top_song_name', color_continuous_scale='Viridis')
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.warning("Chưa có dữ liệu Mart. Hãy bấm 'Load Mart'.")

# ==================================================
# TAB 3: NGƯỜI DÙNG
# ==================================================
with tabs[2]:
    col_a, col_b = st.columns(2)
    
    with col_a:
        st.subheader("Tỉ lệ tài khoản")
        q_level = f"SELECT level, COUNT(DISTINCT user_id) as count FROM songplays JOIN time ON songplays.start_time = time.start_time WHERE time.year = {selected_year} GROUP BY level"
        df_level = query_db(q_level)
        if not df_level.empty:
            fig_pie = px.pie(df_level, names='level', values='count', hole=0.4, color_discrete_sequence=px.colors.sequential.RdBu)
            st.plotly_chart(fig_pie, use_container_width=True)
            
    with col_b:
        st.subheader("Giờ nghe nhạc phổ biến")
        q_heat = f"""
            SELECT t.weekday, t.hour, COUNT(*) as plays
            FROM songplays sp JOIN time t ON sp.start_time = t.start_time
            WHERE t.year = {selected_year}
            GROUP BY t.weekday, t.hour
        """
        df_heat = query_db(q_heat)
        if not df_heat.empty:
            pivot = df_heat.pivot(index='weekday', columns='hour', values='plays').fillna(0)
            pivot = pivot.reindex(index=range(7), columns=range(24), fill_value=0)
            fig_heat = px.imshow(
                pivot, 
                labels=dict(x="Giờ", y="Thứ", color="Lượt nghe"),
                x=list(range(24)), 
                y=['T2','T3','T4','T5','T6','T7','CN'], 
                aspect="auto"
            )
            st.plotly_chart(fig_heat, use_container_width=True)

# ==================================================
# TAB 4: ĐỊA LÝ
# ==================================================
with tabs[3]:
    st.subheader("🗺️ Bản đồ nhiệt phân bố người dùng")
    q_loc = f"""
        SELECT location, COUNT(*) as plays 
        FROM songplays sp JOIN time t ON sp.start_time = t.start_time
        WHERE t.year = {selected_year}
        GROUP BY location ORDER BY plays DESC LIMIT 20
    """
    df_loc = query_db(q_loc)
    if not df_loc.empty:
        st.bar_chart(df_loc.set_index('location'))
    else:
        st.info("Chưa có dữ liệu location.")

# ==================================================
# TAB 5: QUẢN TRỊ & LOGS (ĐÃ CẬP NHẬT TÌM KIẾM)
# ==================================================
with tabs[4]:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("🏥 Health Check")
        null_artist = query_db("SELECT COUNT(*) as c FROM songplays WHERE artist_id IS NULL").iloc[0]['c']
        if null_artist > 0:
            st.error(f"⚠️ Có {null_artist} dòng dữ liệu lỗi (Thiếu Artist ID). Vui lòng chạy 'Fix Data'.")
        else:
            st.success("✅ Dữ liệu Warehouse sạch sẽ!")

    with c2:
        st.subheader("📝 Quản lý Nhật ký (ETL Logs)")
        
        # --- TÍNH NĂNG MỚI: BỘ LỌC LOG ---
        col_filter1, col_filter2 = st.columns([1, 2])
        with col_filter1:
            log_status = st.selectbox("Trạng thái:", ["ALL", "SUCCESS", "FAILED", "RUNNING"])
        
        # Query động theo bộ lọc
        base_log_query = "SELECT log_id, package_name, start_time, status, error_message FROM etl_logs"
        if log_status != "ALL":
            base_log_query += f" WHERE status = '{log_status}'"
        
        base_log_query += " ORDER BY start_time DESC LIMIT 50"
        
        df_log = query_db(base_log_query, "control")
        
        if not df_log.empty:
            def color_status(val):
                if val == 'SUCCESS': return 'background-color: #90EE90'
                if val == 'FAILED': return 'background-color: #FFB6C1'
                return 'background-color: #FFFFE0' # RUNNING/OTHER
                
            st.dataframe(df_log.style.applymap(color_status, subset=['status']), use_container_width=True)
        else:
            st.info("Không tìm thấy log phù hợp.")

# Footer
st.markdown("---")
st.caption("© 2024 Music Data Warehouse | Powered by **Streamlit** & **MySQL**")