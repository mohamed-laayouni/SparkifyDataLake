import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table
    (artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
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
    userId INTEGER)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table
    (num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year FLOAT)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
( songplay_id INTEGER IDENTITY (1,1) PRIMARY KEY,
start_time TIMESTAMP, 
user_id INT NOT NULL, 
level VARCHAR, 
song_id VARCHAR, 
artist_id VARCHAR, 
session_id INT, 
location VARCHAR, 
user_agent VARCHAR)
DISTSTYLE KEY
DISTKEY (start_time)
SORTKEY (start_time);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id INT PRIMARY KEY, 
first_name VARCHAR, 
last_name VARCHAR, 
gender VARCHAR, 
level VARCHAR)
SORTKEY (user_id);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR PRIMARY KEY NOT NULL, 
title VARCHAR NOT NULL, 
artist_id VARCHAR NOT NULL, 
year INT, 
duration FLOAT)
SORTKEY (song_id);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR PRIMARY KEY NOT NULL, 
name VARCHAR NOT NULL, 
location VARCHAR, 
latitude FLOAT, 
longitude FLOAT)
SORTKEY (artist_id);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP PRIMARY KEY, 
hour INT, 
day INT, 
week INT, 
month INT, 
year INT ENCODE BYTEDICT, 
weekday VARCHAR ENCODE BYTEDICT)
DISTSTYLE KEY
DISTKEY (start_time)
SORTKEY (start_time);
""")

# STAGING TABLES
# GETTING DATA FROM S3 AND COPYING INTO FINAL TABLES
# USING THE COPY STATEMENT FROM REDSHIFT WITH LOG AND SONG PATHS AS FOUND IN dwh.cfg

staging_events_copy = ("""
COPY staging_events_table FROM {} iam_role {} region 'us-west-2' FORMAT AS json {} 
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs_table FROM {} iam_role {} region 'us-west-2' FORMAT AS json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, LEVEL, song_id, artist_id, session_id, LOCATION, user_agent)
SELECT TIMESTAMP 'epoch' + (set.ts / 1000) * INTERVAL '1 second' AS start_time,
       set.userId,
       set.level,
       sst.song_id,
       sst.artist_id,
       set.sessionId,
       set.location,
       set.userAgent
FROM staging_songs_table sst
JOIN staging_events_table set ON (sst.title = set.song AND sst.artist_name=set.artist)
AND set.page ='NextSong';
""")

user_table_insert = ("""
INSERT INTO users
WITH unique_staging_events AS (
    SELECT userId, firstName, lastName, gender, level, ROW_NUMBER() OVER(PARTITION BY userId ORDER BY ts DESC) AS rank
    FROM staging_events_table
    WHERE userId IS NOT NULL
    AND page = 'NextSong')
SELECT userId,
       firstName,
       lastName,
       gender,
       level
FROM unique_staging_events
WHERE rank = 1;
""")

song_table_insert = ("""
INSERT INTO songs
SELECT song_id,
       title,
       artist_id,
       YEAR,
       duration
FROM staging_songs_table
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT artist_id,
       artist_name,
       artist_location,
       artist_latitude,
       artist_longitude
FROM staging_songs_table;
""")

time_table_insert = ("""
INSERT INTO TIME
SELECT DISTINCT TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' AS start_time,
       EXTRACT(HOUR
               FROM start_time) AS HOUR,
       EXTRACT(DAY
               FROM start_time) AS DAY,
       EXTRACT(WEEKS
               FROM start_time) AS WEEK,
       EXTRACT(MONTH
               FROM start_time) AS MONTH,
       EXTRACT(YEAR
               FROM start_time) AS YEAR,
       to_char(start_time, 'Day') AS weekday
FROM staging_events_table;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
