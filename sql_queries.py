import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER
)
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude VARCHAR,
    artist_longitude VARCHAR,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INTEGER
)
""")

songplay_table_create = ("""CREATE TABLE songplays (
    songplay_id INTEGER IDENTITY(0, 1),
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR,
    PRIMARY KEY (songplay_id),
    FOREIGN KEY (start_time) REFERENCES time (start_time),
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (song_id) REFERENCES songs (song_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
)
""")

user_table_create = ("""CREATE TABLE users (
    user_id INTEGER NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR,
    PRIMARY KEY (user_id)
)
""")

song_table_create = ("""CREATE TABLE songs (
    song_id VARCHAR NOT NULL,
    title VARCHAR,
    artist_id VARCHAR NOT NULL,
    year INT,
    duration FLOAT,
    PRIMARY KEY (song_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
)
""")

artist_table_create = ("""CREATE TABLE artists (
    artist_id VARCHAR NOT NULL,
    name VARCHAR,
    location VARCHAR,
    latitude VARCHAR,
    longitude VARCHAR,
    PRIMARY KEY (artist_id)
)
""")

time_table_create = ("""CREATE TABLE time (
    start_time TIMESTAMP NOT NULL,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER,
    PRIMARY KEY (start_time)
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {}
IAM_ROLE {}
JSON {}
REGION {}
COMPUPDATE OFF STATUPDATE OFF
""").format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"), config.get("S3", "REGION"))

staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE {}
REGION {}
FORMAT AS JSON 'auto'
COMPUPDATE OFF STATUPDATE OFF
""").format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "REGION"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO SONGPLAYS (
      start_time,
      user_id,
      level,
      song_id,
      artist_id,
      session_id,
      location,
      user_agent
)
SELECT DISTINCT
    timestamp 'epoch' + se.ts / 1000 * interval '1 second' as start_time,
    se.userId as user_id,
    se.level as level,
    ss.song_id as song_id,
    ss.artist_id as artist_id,
    se.sessionId as session_id,
    se.location as location,
    se.userAgent as user_agent
FROM staging_events se 
LEFT JOIN staging_songs ss ON (se.song=ss.title AND se.artist=ss.artist_name)
WHERE se.page='NextSong'
""")

user_table_insert = ("""
INSERT INTO users 
(
    SELECT DISTINCT 
         userId as user_id, 
         firstName as first_name, 
         lastName as last_name, 
         gender as gender, 
         level as level 
    FROM staging_events
    WHERE userId IS NOT NULL AND page='NextSong'
)
""")

song_table_insert = ("""
INSERT INTO songs
(
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
)
""")

artist_table_insert = ("""
INSERT INTO artists 
(SELECT DISTINCT 
     artist_id, 
     artist_name as name, 
     artist_location as location, 
     artist_latitude as latitude, 
     artist_longitude as longitude 
FROM staging_songs
)
""")

time_table_insert = ("""
INSERT INTO time
(
    SELECT 
        start_time, 
        date_part(h, start_time) as hour,
        date_part(d, start_time) as day,
        date_part(w, start_time) as week,
        date_part(mon, start_time) as month,
        date_part(y, start_time) as year,
        date_part(dayofweek, start_time) as weekday
    FROM (SELECT DISTINCT 
            timestamp 'epoch' + ts / 1000 * interval '1 second' as start_time
          FROM staging_events)
)
""")

# QUERY LISTS

create_staging_table_queries = [staging_events_table_create, staging_songs_table_create]
create_fact_dim_table_queries = [artist_table_create, user_table_create, song_table_create, time_table_create, songplay_table_create]
create_table_queries = create_staging_table_queries + create_fact_dim_table_queries

drop_staging_table_queries = [staging_events_table_drop, staging_songs_table_drop]
drop_fact_dim_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
drop_table_queries = drop_staging_table_queries + drop_fact_dim_table_queries

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, time_table_insert, song_table_insert, artist_table_insert, songplay_table_insert]
