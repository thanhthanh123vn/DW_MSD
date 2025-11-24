# dashboard_advanced.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import subprocess
import os
import sys
from db import create_connection_Mart

# --- CẤU HÌNH TRANG ---
st.set_page_config(
    page_title="Data Warehouse Monitor Pro",
    page_icon="🎧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CSS TÙY CHỈNH ---
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 15px;
        text-align: center;
    }
    .stButton>button {
        width: 100%;
    }
</style>
""", unsafe_allow_html=True)

# --- HÀM HỖ TRỢ ---
@st.cache_data(ttl=300)
def run_query(query, params=None):
    """Chạy SQL query an toàn và trả về DataFrame."""
    conn = None
    try:
        cur, conn = create_connection()
        cur.execute(query, params or ())
        if cur.description:
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()
            return pd.DataFrame(data, columns=columns)
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Lỗi SQL: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()

def run_script(script_name):
    """Hàm chạy script python từ giao diện."""
    try:
        result = subprocess.run(
            [sys.executable, "-m", script_name],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        if result.returncode == 0:
            st.success(f"✅ Chạy {script_name} thành công!")
            st.code(result.stdout)
        else:
            st.error(f"❌ Lỗi khi chạy {script_name}")
            st.code(result.stderr)
    except Exception as e:
        st.error(f"Không thể chạy script: {e}")

# --- SIDEBAR ---
with st.sidebar:
    st.title("🎛️ Điều khiển")
    
    st.subheader("Bộ lọc dữ liệu")
    filter_level = st.multiselect(
        "Chọn loại tài khoản:",
        options=["free", "paid"],
        default=["free", "paid"]
    )
    
    df_years = run_query("SELECT DISTINCT year FROM time ORDER BY year DESC")
    selected_year = st.selectbox("Chọn năm dữ liệu:", df_years['year']) if not df_years.empty else None

    st.markdown("---")
    st.caption("System Status: 🟢 Online")

# --- GIAO DIỆN CHÍNH ---
st.title("🎧 Music Streaming Data Warehouse")

tab1, tab2, tab3 = st.tabs(["📊 Tổng Quan (Overview)", "🔎 Phân Tích Sâu (Analytics)", "⚙️ Quản Trị (Ops)"])

# ==========================================
# TAB 1: TỔNG QUAN
# ==========================================
with tab1:
    st.markdown("### 📈 Chỉ số quan trọng (KPIs)")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # Sử dụng try-except để tránh lỗi nếu bảng chưa có dữ liệu
        try:
            count_plays = run_query("SELECT COUNT(*) as c FROM songplays").iloc[0]['c']
            st.metric("Tổng lượt nghe", f"{count_plays:,}")
        except:
            st.metric("Tổng lượt nghe", "0")
        
    with col2:
        try:
            count_users = run_query("SELECT COUNT(*) as c FROM users").iloc[0]['c']
            st.metric("Người dùng", f"{count_users}")
        except:
            st.metric("Người dùng", "0")

    with col3:
        try:
            count_songs = run_query("SELECT COUNT(*) as c FROM songs").iloc[0]['c']
            st.metric("Kho nhạc (Bài)", f"{count_songs:,}")
        except:
             st.metric("Kho nhạc (Bài)", "0")

    with col4:
        try:
            avg_duration = run_query("SELECT AVG(duration) as c FROM songs").iloc[0]['c']
            val = avg_duration if avg_duration else 0
            st.metric("Thời lượng TB", f"{round(val/60, 2)} phút")
        except:
             st.metric("Thời lượng TB", "0 phút")

    st.markdown("---")

    st.subheader("📅 Xu hướng lượt nghe theo thời gian")
    trend_query = "SELECT date, total_plays FROM mart_daily_plays ORDER BY date"
    df_trend = run_query(trend_query)
    if not df_trend.empty:
        fig = px.area(df_trend, x='date', y='total_plays', 
                      title="Biểu đồ vùng: Số lượt nghe hàng ngày",
                      color_discrete_sequence=['#FF4B4B'])
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Chưa có dữ liệu Mart. Hãy chạy 'Load Mart' ở tab Quản trị.")

# ==========================================
# TAB 2: PHÂN TÍCH SÂU
# ==========================================
with tab2:
    row1_col1, row1_col2 = st.columns([2, 1])
    
    with row1_col1:
        st.subheader("🗺️ Bản đồ phân bố Nghệ sĩ")
        map_query = """
            SELECT name, location, latitude, longitude
            FROM artists 
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            LIMIT 500
        """
        df_map = run_query(map_query)
        if not df_map.empty:
            st.map(df_map, latitude='latitude', longitude='longitude')
        else:
            st.info("Dữ liệu nghệ sĩ chưa có tọa độ.")

    with row1_col2:
        st.subheader("🖥️ User Agent (Thiết bị)")
        ua_query = """
            SELECT 
                CASE 
                    WHEN user_agent LIKE '%Macintosh%' THEN 'Mac'
                    WHEN user_agent LIKE '%Windows%' THEN 'Windows'
                    WHEN user_agent LIKE '%Linux%' THEN 'Linux'
                    WHEN user_agent LIKE '%iPhone%' THEN 'iPhone'
                    ELSE 'Other'
                END as os,
                COUNT(*) as count
            FROM songplays
            GROUP BY os
        """
        df_ua = run_query(ua_query)
        if not df_ua.empty:
            fig_donut = px.pie(df_ua, names='os', values='count', hole=0.4)
            st.plotly_chart(fig_donut, use_container_width=True)

    st.markdown("---")
    
    st.subheader("🔥 Heatmap: Thói quen nghe nhạc")
    st.caption("Trục dọc: Thứ trong tuần (0=Thứ 2), Trục ngang: Giờ trong ngày")
    
    heat_query = """
        SELECT weekday, hour, COUNT(*) as plays
        FROM time
        GROUP BY weekday, hour
    """
    df_heat = run_query(heat_query)
    
    if not df_heat.empty:
        # Pivot table
        heatmap_data = df_heat.pivot(index='weekday', columns='hour', values='plays')
        
        # --- FIX LỖI VALUE ERROR TẠI ĐÂY ---
        # Tạo khung dữ liệu chuẩn đủ 7 ngày và 24 giờ
        full_weekdays = range(7) 
        full_hours = range(24)
        
        # Reindex: Ép dữ liệu phải có đủ các dòng/cột này, thiếu thì điền 0
        heatmap_data = heatmap_data.reindex(index=full_weekdays, columns=full_hours, fill_value=0)
        # -----------------------------------

        fig_heat = px.imshow(heatmap_data, 
                             labels=dict(x="Giờ", y="Thứ", color="Lượt nghe"),
                             x=heatmap_data.columns,
                             y=['T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'CN'], # Dữ liệu đã đủ 7 dòng, khớp với 7 nhãn này
                             color_continuous_scale='Viridis',
                             aspect="auto")
        st.plotly_chart(fig_heat, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu thời gian.")

# ==========================================
# TAB 3: QUẢN TRỊ HỆ THỐNG
# ==========================================
with tab3:
    st.header("🛠️ Control Panel")
    
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    
    if c1.button("1. Create Tables"):
        with st.spinner("Đang tạo bảng..."):
            run_script("create_tables")
            
    if c2.button("2. Extract Data"):
        with st.spinner("Đang giải nén..."):
            run_script("extraction.extract")
            
    if c3.button("3. Load Staging"):
        with st.spinner("Đang load staging..."):
            run_script("load.load_staging")
            
    if c4.button("4. Load Warehouse"):
        with st.spinner("Đang load warehouse..."):
            run_script("load.load_warehouse")

    if c5.button("5. Transform"):
        with st.spinner("Đang transform..."):
            run_script("transform.create_aggregate")
            
    if c6.button("6. Load Mart"):
        with st.spinner("Đang tạo báo cáo..."):
            run_script("load.load_mart")
            st.cache_data.clear()

    st.markdown("---")
    st.subheader("📝 Nhật ký hệ thống (ETL Logs)")
    
    log_filter = st.radio("Trạng thái log:", ["ALL", "SUCCESS", "FAILED"], horizontal=True)
    
    base_log_query = """
        SELECT log_id, package_name, start_time, end_time, status, 
               rows_extracted, rows_loaded, error_message
        FROM etl_logs
    """
    
    if log_filter != "ALL":
        base_log_query += f" WHERE status = '{log_filter}'"
        
    base_log_query += " ORDER BY start_time DESC LIMIT 50"
    
    df_logs = run_query(base_log_query)
    
    if not df_logs.empty:
        st.dataframe(
            df_logs, 
            use_container_width=True,
            column_config={
                "status": st.column_config.TextColumn(
                    "Trạng thái",
                    validate="^(SUCCESS|FAILED|RUNNING)$"
                ),
                "error_message": "Chi tiết lỗi"
            }
        )
    else:
        st.info("Chưa có log nào.")

# Footer
st.markdown("---")
st.markdown("© 2024 Data Warehouse Project | Powered by **Streamlit** & **MySQL**")