# sql_queries.py

# --- 1. CONTROL DATABASE ---
create_control_queries = [
    """
    CREATE TABLE IF NOT EXISTS etl_logs (
        log_id INT AUTO_INCREMENT PRIMARY KEY,
        package_name VARCHAR(255),
        start_time DATETIME,
        end_time DATETIME,
        status VARCHAR(50),
        rows_extracted INT DEFAULT 0,
        rows_loaded INT DEFAULT 0,
        rows_rejected INT DEFAULT 0,
        error_message TEXT
    );
    """
]

# --- 2. WAREHOUSE DATABASE ---
create_warehouse_queries = [
    # (Giữ nguyên các bảng artists, songs, users, time, songplays như cũ)
    """
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(255) PRIMARY KEY,
        name VARCHAR(255),
        location VARCHAR(255),
        latitude DOUBLE,
        longitude DOUBLE,
        load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(255) PRIMARY KEY,
        title VARCHAR(255),
        artist_id VARCHAR(255),
        year INT,
        duration DOUBLE,
        FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
        load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        gender VARCHAR(10),
        level VARCHAR(50),
        load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS time (
        start_time DATETIME PRIMARY KEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT,
        load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id VARCHAR(36) PRIMARY KEY,
        start_time DATETIME,
        user_id INT,
        level VARCHAR(50),
        song_id VARCHAR(255),
        artist_id VARCHAR(255),
        session_id INT,
        location VARCHAR(255),
        user_agent VARCHAR(512),
        load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
]

# --- 3. MART DATABASE (ĐÃ CẬP NHẬT) ---
create_mart_queries = [
    # Bảng tổng hợp lượt nghe theo ngày
    """
    CREATE TABLE IF NOT EXISTS songplays_daily (
        date DATE PRIMARY KEY,
        play_count INT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    # Bảng Top Nghệ sĩ (Thêm thông tin bài hát Hit)
    """
    CREATE TABLE IF NOT EXISTS top_artists (
        artist_id VARCHAR(255) PRIMARY KEY,
        artist_name VARCHAR(255),
        location VARCHAR(255),
        total_plays INT,               -- Tổng lượt nghe của nghệ sĩ
        top_song_name VARCHAR(255),    -- Tên bài hát được nghe nhiều nhất của họ
        top_song_plays INT,            -- Lượt nghe của bài hát đó
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    # Bảng Tổng quan hệ thống
    """
    CREATE TABLE IF NOT EXISTS system_overview (
        metric_name VARCHAR(50) PRIMARY KEY,
        metric_value VARCHAR(255),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
]

# --- DATAMART INSERT LOGIC (MỚI) ---
# Logic: Tính tổng lượt nghe nghệ sĩ + Tìm bài hát top 1 của nghệ sĩ đó

mart_top_artists_insert = """
INSERT INTO top_artists (artist_id, artist_name, location, total_plays, top_song_name, top_song_plays)
WITH 
-- 1. Thống kê tổng lượt nghe theo Nghệ sĩ
ArtistStats AS (
    SELECT 
        a.artist_id, 
        a.name, 
        a.location, 
        COUNT(sp.songplay_id) as total_plays
    FROM database_warehouse.songplays sp
    JOIN database_warehouse.artists a ON sp.artist_id = a.artist_id
    GROUP BY a.artist_id, a.name, a.location
),
-- 2. Thống kê lượt nghe từng Bài hát và Xếp hạng trong nội bộ Nghệ sĩ
SongStats AS (
    SELECT 
        sp.artist_id, 
        s.title, 
        COUNT(sp.songplay_id) as song_plays,
        -- Hàm ROW_NUMBER để lấy bài hát top 1 của mỗi nghệ sĩ
        ROW_NUMBER() OVER (PARTITION BY sp.artist_id ORDER BY COUNT(sp.songplay_id) DESC) as rn
    FROM database_warehouse.songplays sp
    JOIN database_warehouse.songs s ON sp.song_id = s.song_id
    GROUP BY sp.artist_id, s.title
)
-- 3. Kết hợp lại để lấy thông tin đầy đủ
SELECT 
    ast.artist_id,
    ast.name,
    COALESCE(ast.location, 'Unknown') as location,
    ast.total_plays,
    ss.title as top_song_name,
    ss.song_plays as top_song_plays
FROM ArtistStats ast
LEFT JOIN SongStats ss ON ast.artist_id = ss.artist_id AND ss.rn = 1
ORDER BY ast.total_plays DESC
LIMIT 100
ON DUPLICATE KEY UPDATE 
    total_plays = VALUES(total_plays),
    top_song_name = VALUES(top_song_name),
    top_song_plays = VALUES(top_song_plays),
    last_updated = NOW();
"""

mart_overview_queries = [
    "INSERT INTO system_overview (metric_name, metric_value) SELECT 'Total Plays', COUNT(*) FROM database_warehouse.songplays ON DUPLICATE KEY UPDATE metric_value = VALUES(metric_value), updated_at = NOW();",
    "INSERT INTO system_overview (metric_name, metric_value) SELECT 'Total Users', COUNT(*) FROM database_warehouse.users ON DUPLICATE KEY UPDATE metric_value = VALUES(metric_value), updated_at = NOW();",
    "INSERT INTO system_overview (metric_name, metric_value) SELECT 'Total Songs', COUNT(*) FROM database_warehouse.songs ON DUPLICATE KEY UPDATE metric_value = VALUES(metric_value), updated_at = NOW();",
    # Nghệ sĩ Top 1 hệ thống
    """INSERT INTO system_overview (metric_name, metric_value) 
       SELECT 'Top Trending Artist', 
              (SELECT name FROM database_warehouse.artists a 
               JOIN database_warehouse.songplays sp ON a.artist_id = sp.artist_id 
               GROUP BY a.name ORDER BY COUNT(*) DESC LIMIT 1)
       ON DUPLICATE KEY UPDATE metric_value = VALUES(metric_value), updated_at = NOW();"""
]

# --- CÁC QUERY KHÁC GIỮ NGUYÊN ---
artist_table_insert = "INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE name = VALUES(name);"
song_table_insert = "INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE title = VALUES(title);"
user_table_insert = "INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE level = VALUES(level);"
time_table_insert = "INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE start_time = start_time;"
songplay_table_insert = "INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE session_id = VALUES(session_id);"
song_select = "SELECT s.song_id, a.artist_id FROM songs s JOIN artists a ON s.artist_id = a.artist_id WHERE s.title = %s AND a.name = %s AND s.duration = %s LIMIT 1;"

etl_log_insert = "INSERT INTO etl_logs (package_name, start_time, status) VALUES (%s, NOW(), 'RUNNING');"
etl_log_update_success = "UPDATE etl_logs SET end_time = NOW(), status = 'SUCCESS', rows_extracted=%s, rows_loaded=%s, rows_rejected=%s WHERE log_id=%s;"
etl_log_update_fail = "UPDATE etl_logs SET end_time = NOW(), status = 'FAILED', error_message=%s WHERE log_id=%s;"