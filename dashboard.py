# dashboard.py
import streamlit as st
import pandas as pd
import plotly.express as px
import mysql.connector
import subprocess
import sys
import os
from config import DB_COMMON, DB_NAMES

# --- CẤU HÌNH TRANG ---
st.set_page_config(
    page_title="Music Data Warehouse Pro", 
    page_icon="🎧", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CSS TÙY CHỈNH CHO ĐẸP ---
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 15px;
        text-align: center;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
    h1, h2, h3 { color: #2E86C1; }
</style>
""", unsafe_allow_html=True)

# --- HÀM HỖ TRỢ ---
@st.cache_data(ttl=60) # Cache 60s để không query liên tục
def get_data(query, db_key):
    """Kết nối và query dữ liệu từ DB cụ thể (Mart hoặc Warehouse)"""
    conn = None
    try:
        conn = mysql.connector.connect(
            host=DB_COMMON["host"],
            user=DB_COMMON["user"],
            password=DB_COMMON["password"],
            port=DB_COMMON["port"],
            database=DB_NAMES[db_key]
        )
        return pd.read_sql(query, conn)
    except Exception as e:
        return pd.DataFrame()
    finally:
        if conn: conn.close()

def run_script(script_name):
    """Chạy script python từ giao diện"""
    try:
        # Tìm đường dẫn tuyệt đối của script
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), script_name)
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            st.toast(f"✅ Chạy {script_name} thành công!", icon="🎉")
            st.success(result.stdout)
        else:
            st.error(f"❌ Lỗi: {result.stderr}")
    except Exception as e:
        st.error(f"Không thể chạy script: {e}")

# --- SIDEBAR ---
with st.sidebar:
    st.title("🎛️ Control Panel")
    st.info("Hệ thống: Online 🟢")
    
    st.markdown("---")
    st.header("⚙️ Tác vụ nhanh")
    if st.button("🛠️ 1. Fix Data (Sửa lỗi 0 dòng)"):
        with st.spinner("Đang đồng bộ dữ liệu thật..."):
            run_script("fix_data.py")
            st.cache_data.clear() # Xóa cache để load lại dữ liệu mới
            
    if st.button("🔄 2. Reload Mart (Cập nhật báo cáo)"):
        with st.spinner("Đang tính toán lại báo cáo..."):
            run_script("load/load_mart.py")
            st.cache_data.clear()

# --- GIAO DIỆN CHÍNH ---
st.title("🎧 Music Streaming Data Warehouse")
st.caption("Báo cáo thông minh từ hệ thống 4 Database (Staging -> Warehouse -> Mart)")

# TABS
tab_overview, tab_artist, tab_deep_dive, tab_logs = st.tabs([
    "📊 Tổng Quan", 
    "🎤 Phân Tích Nghệ Sĩ", 
    "🔎 Thói Quen Người Dùng",
    "📝 Nhật Ký Hệ Thống"
])

# ==================================================
# TAB 1: TỔNG QUAN (Lấy từ MART)
# ==================================================
with tab_overview:
    st.subheader("📈 Chỉ số quan trọng (KPIs)")
    
    # Lấy dữ liệu từ bảng system_overview trong Mart
    try:
        df_overview = get_data("SELECT * FROM system_overview", "mart")
        
        # Helper function
        def get_metric(name):
            if not df_overview.empty:
                row = df_overview[df_overview['metric_name'] == name]
                if not row.empty:
                    return row.iloc[0]['metric_value']
            return "0"

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Tổng lượt nghe", get_metric("Total Plays"), delta="All time")
        c2.metric("Người dùng", get_metric("Total Users"))
        c3.metric("Kho nhạc", get_metric("Total Songs"))
        c4.metric("Top Trending", get_metric("Top Trending Artist"))
        
    except Exception as e:
        st.warning("⚠️ Chưa có dữ liệu Overview. Vui lòng bấm 'Fix Data' bên trái.")

    st.markdown("---")
    st.subheader("📅 Xu hướng nghe nhạc theo ngày")
    
    df_daily = get_data("SELECT date, play_count FROM songplays_daily ORDER BY date", "mart")
    if not df_daily.empty:
        fig_trend = px.area(df_daily, x='date', y='play_count', 
                            title="Số lượt nghe hàng ngày", markers=True,
                            color_discrete_sequence=['#FF4B4B'])
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu lịch sử nghe.")

# ==================================================
# TAB 2: NGHỆ SĨ (Lấy từ MART + WAREHOUSE)
# ==================================================
with tab_artist:
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("🏆 Top 10 Nghệ sĩ & Bài hát Hit")
        df_top = get_data("""
            SELECT artist_name, total_plays, top_song_name, top_song_plays 
            FROM top_artists 
            ORDER BY total_plays DESC LIMIT 10
        """, "mart")
        
        if not df_top.empty:
            fig_bar = px.bar(
                df_top, y='artist_name', x='total_plays', 
                orientation='h',
                text='top_song_name',
                title="Xếp hạng nghệ sĩ (kèm bài hát nổi bật)",
                labels={'total_plays': 'Lượt nghe', 'artist_name': 'Nghệ sĩ'},
                color='total_plays', color_continuous_scale='Viridis'
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.warning("Dữ liệu trống. Hãy chạy 'Fix Data' và 'Reload Mart'.")

    with col2:
        st.subheader("🗺️ Bản đồ phân bố")
        st.caption("Nghệ sĩ đến từ đâu?")
        # Lấy location từ Mart (đã được làm giàu)
        df_loc = get_data("""
            SELECT location, total_plays 
            FROM top_artists 
            WHERE location IS NOT NULL AND location != 'Unknown'
            LIMIT 50
        """, "mart")
        
        if not df_loc.empty:
            # Vì location dạng text, ta chỉ hiện bảng thống kê (muốn hiện map cần tọa độ từ Warehouse)
            st.dataframe(df_loc, use_container_width=True, hide_index=True)
        else:
            st.info("Chưa có thông tin địa điểm.")

# ==================================================
# TAB 3: THÓI QUEN (Deep Dive vào WAREHOUSE)
# ==================================================
with tab_deep_dive:
    st.markdown("### 🕵️ Phân tích hành vi (Truy vấn trực tiếp Warehouse)")
    
    col_heat, col_os = st.columns([2, 1])
    
    with col_heat:
        st.subheader("🔥 Heatmap: Giờ vàng nghe nhạc")
        # Query phức tạp này lấy từ Warehouse
        heatmap_query = """
            SELECT t.weekday, t.hour, COUNT(*) as plays
            FROM songplays s
            JOIN time t ON s.start_time = t.start_time
            GROUP BY t.weekday, t.hour
        """
        df_heat = get_data(heatmap_query, "warehouse") # Query Warehouse
        
        if not df_heat.empty:
            # Pivot data
            pivot_heat = df_heat.pivot(index='weekday', columns='hour', values='plays').fillna(0)
            # Reindex cho đủ 7 ngày 24h
            pivot_heat = pivot_heat.reindex(index=range(7), columns=range(24), fill_value=0)
            
            fig_heat = px.imshow(
                pivot_heat,
                labels=dict(x="Giờ trong ngày", y="Thứ (0=T2, 6=CN)", color="Lượt nghe"),
                x=range(24),
                y=['T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'CN'],
                aspect="auto", color_continuous_scale='Magma'
            )
            st.plotly_chart(fig_heat, use_container_width=True)
        else:
            st.info("Chưa có dữ liệu thời gian chi tiết trong Warehouse.")

    with col_os:
        st.subheader("📱 Thiết bị sử dụng")
        # Phân tích User Agent đơn giản
        ua_query = """
            SELECT 
                CASE 
                    WHEN user_agent LIKE '%Windows%' THEN 'Windows'
                    WHEN user_agent LIKE '%Mac%' THEN 'Mac'
                    WHEN user_agent LIKE '%Linux%' THEN 'Linux'
                    ELSE 'Mobile/Other'
                END as platform,
                COUNT(*) as count
            FROM songplays
            GROUP BY platform
        """
        df_ua = get_data(ua_query, "warehouse")
        if not df_ua.empty:
            fig_pie = px.pie(df_ua, names='platform', values='count', hole=0.4)
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("Chưa có dữ liệu User Agent.")

# ==================================================
# TAB 4: LOGS (Lấy từ CONTROL)
# ==================================================
with tab_logs:
    st.subheader("📝 Nhật ký ETL gần nhất")
    
    log_filter = st.radio("Lọc trạng thái:", ["ALL", "SUCCESS", "FAILED"], horizontal=True)
    
    base_query = "SELECT log_id, package_name, start_time, status, error_message FROM etl_logs"
    if log_filter != "ALL":
        base_query += f" WHERE status = '{log_filter}'"
    base_query += " ORDER BY start_time DESC LIMIT 20"
    
    df_logs = get_data(base_query, "control") # Query Control DB
    
    if not df_logs.empty:
        # Tô màu trạng thái
        def color_status(val):
            color = '#90EE90' if val == 'SUCCESS' else '#FFB6C1' if val == 'FAILED' else '#FFFFE0'
            return f'background-color: {color}'

        st.dataframe(df_logs.style.applymap(color_status, subset=['status']), use_container_width=True)
    else:
        st.info("Chưa có log nào.")

# Footer
st.markdown("---")
st.markdown("© 2024 Music Warehouse | Powered by **Streamlit** & **MySQL**")