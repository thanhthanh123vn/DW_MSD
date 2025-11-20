# sql_queries.py

# --- 1. DANH SÁCH DROP (Xóa bảng cũ nếu có) ---
drop_table_queries = [
    "DROP TABLE IF EXISTS songplays;",
    "DROP TABLE IF EXISTS users;",
    "DROP TABLE IF EXISTS time;",
    "DROP TABLE IF EXISTS songs;",
    "DROP TABLE IF EXISTS artists;",
    "DROP TABLE IF EXISTS etl_logs;"  # <--- Thêm dòng này
]

# --- 2. DANH SÁCH CREATE (Tạo bảng mới) ---
create_table_queries = [
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
        FOREIGN KEY (start_time) REFERENCES time(start_time),
        FOREIGN KEY (user_id) REFERENCES users(user_id),
        load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    # --- BẢNG MỚI: ETL LOGS ---
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

# --- 3. CÁC CÂU LỆNH INSERT DỮ LIỆU ---

# (Giữ nguyên các câu insert cũ: artist_table_insert, song_table_insert, ...)
artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      name = VALUES(name), location = VALUES(location),
      latitude = VALUES(latitude), longitude = VALUES(longitude);
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      title = VALUES(title), artist_id = VALUES(artist_id),
      year = VALUES(year), duration = VALUES(duration);
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      first_name = VALUES(first_name), last_name = VALUES(last_name),
      gender = VALUES(gender), level = VALUES(level);
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE start_time = start_time;
""")

songplay_table_insert = ("""
    INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE session_id = VALUES(session_id);
""")

song_select = ("""
    SELECT s.song_id, a.artist_id
    FROM songs s
    JOIN artists a ON s.artist_id = a.artist_id
    WHERE s.title = %s AND a.name = %s AND s.duration = %s
    LIMIT 1;
""")

# --- 4. QUERIES CHO LOGGING (MỚI) ---
etl_log_insert = ("""
    INSERT INTO etl_logs (package_name, start_time, status)
    VALUES (%s, NOW(), 'RUNNING');
""")

etl_log_update_success = ("""
    UPDATE etl_logs
    SET end_time = NOW(),
        status = 'SUCCESS',
        rows_extracted = %s,
        rows_loaded = %s,
        rows_rejected = %s
    WHERE log_id = %s;
""")

etl_log_update_fail = ("""
    UPDATE etl_logs
    SET end_time = NOW(),
        status = 'FAILED',
        error_message = %s
    WHERE log_id = %s;
""")