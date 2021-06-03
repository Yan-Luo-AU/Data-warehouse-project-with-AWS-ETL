import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    event_id BIGINT IDENTITY(0,1) NOT NULL,
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR,
    itemInSession VARCHAR,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionid INT NOT NULL,
    song VARCHAR,
    status INT,
    ts BIGINT NOT NULL,
    userAgent VARCHAR,
    userId INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id VARCHAR NOT NULL,
    artist_latitude VARCHAR,
    artist_longitude VARCHAR,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR NOT NULL,
    title VARCHAR,
    duration FLOAT,
    year INT
)
""")

songplay_table_create = ("""
   CREATE TABLE IF NOT EXISTS songplay(
       songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY, 
       start_time TIMESTAMP NOT NULL, 
       user_id VARCHAR NOT NULL, 
       level VARCHAR, 
       song_id VARCHAR NOT NULL, 
       artist_id VARCHAR NOT NULL, 
       session_id VARCHAR NOT NULL, 
       location VARCHAR, 
       user_agent VARCHAR
)
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id VARCHAR PRIMARY KEY, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender VARCHAR, 
        level VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY, 
        title VARCHAR, 
        artist_id VARCHAR, 
        year INT, 
        duration DECIMAL(9)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY, 
        artist_name VARCHAR, 
        artist_location VARCHAR, 
        artist_latitude VARCHAR, 
        artist_longitude VARCHAR
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY, 
        hour SMALLINT, 
        day SMALLINT, 
        week SMALLINT, 
        month SMALLINT, 
        year SMALLINT, 
        weekday SMALLINT
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    STATUPDATE ON
    FORMAT AS JSON {};
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs from {}
    credentials 'aws_iam_role={}'
    json 'auto'
    REGION 'us-west-2'
    JSON 'auto'
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay(
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent)
SELECT DISTINCT timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time,
       se.userId, 
       se.level, 
       ss.song_id, 
       ss.artist_id,
       se.sessionId AS session_id,
       se.location,
       se.userAgent AS user_agent
FROM staging_events se
JOIN staging_songs ss ON (se.artist = ss.artist_name)
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users(
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level)
SELECT DISTINCT userId as user_id, firstName as first_name, lastName as last_name, gender, level
FROM staging_events
WHERE page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs(
    song_id, 
    title, 
    artist_id, 
    year, 
    duration)
SELECT DISTINCT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
FROM staging_songs ss
JOIN staging_events se ON (se.artist = ss.artist_name)
WHERE se.page = 'NextSong';
""")

artist_table_insert = ("""
INSERT INTO artists(
    artist_id, 
    artist_name, 
    artist_latitude, 
    artist_longitude,
    artist_location)
SELECT DISTINCT artist_id, artist_name, artist_latitude, artist_longitude, artist_location
FROM staging_songs
WHERE page = 'NextSong';
""")

time_table_insert = ("""
INSERT INTO time(
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday)
SELECT DISTINCT timestamp 'epoch' + ts/1000 * interval '1 second' as start_time,
       EXTRACT(hour FROM start_time) AS hour, 
       EXTRACT(day FROM start_time) AS day,
       EXTRACT(week FROM start_time) AS week,
       EXTRACT(month FROM start_time) AS month,
       EXTRACT(year FROM start_time) AS year,
       EXTRACT(weekday FROM start_time) AS weekday
FROM staging_events
WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
