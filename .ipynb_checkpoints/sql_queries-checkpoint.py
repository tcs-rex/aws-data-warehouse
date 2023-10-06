# import libraries
import configparser


# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE STAGING TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
artist TEXT,
auth TEXT,
firstName TEXT,
gender TEXT,
itemInSession INT,
lastName TEXT,
length FLOAT,
level TEXT,
location TEXT,
method TEXT,
page TEXT,
registration DOUBLE PRECISION,
sessionId INT,
song TEXT,
status INT,
ts bigint,
userAgent TEXT,
userId INT);""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
num_songs INT,
artist_id TEXT,
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location TEXT,
artist_name TEXT,
song_id TEXT,
title TEXT,
duration FLOAT,
year INT);""")


# CREATE FACT TABLE

songplay_table_create = ("""
CREATE TABLE songplays(
songplay_id INT identity(0,1) PRIMARY KEY,
start_time TIMESTAMP,
user_id INT,
level TEXT,
song_id TEXT,
artist_id TEXT,
session_id INT,
location TEXT,
user_agent TEXT);""")


# CREATE DIMENSION TABLES

user_table_create = ("""
CREATE TABLE users(
user_id INT NOT NULL PRIMARY KEY,
first_name TEXT NOT NULL,
last_name TEXT NOT NULL,
gender VARCHAR(1),
level TEXT);""")

song_table_create = ("""
CREATE TABLE songs(
song_id TEXT NOT NULL PRIMARY KEY,
title TEXT,
artist_id TEXT,
year INT,
duration FLOAT);""")

artist_table_create = ("""
CREATE TABLE artists(
artist_id TEXT NOT NULL PRIMARY KEY,
name TEXT NOT NULL,
location TEXT,
latitude TEXT,
longitude TEXT);""")

time_table_create = ("""
CREATE TABLE time(
instance_id INT IDENTITY(0,1) PRIMARY KEY,
start_time TIMESTAMP,
hour INT,
day INT,
week INT,
month INT,
year INT,
weekday INT);""")


# STAGING TABLES COPY QUERIES

staging_events_copy = ("""COPY staging_events FROM '{}' IAM_ROLE '{}' JSON '{}' REGION 'us-west-2';""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""COPY staging_songs FROM '{}' IAM_ROLE '{}' JSON 'auto ignorecase' region 'us-west-2';""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

            
# FINAL FACT AND DIMENSION TABLES
            
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP WITHOUT TIME ZONE 'epoch' + (e.ts / 1000) * INTERVAL '1 second' as start_time, e.userId as user_id, e.level, s.song_id, s.artist_id, e.sessionId as session_id, e.location, e.userAgent as user_agent
FROM staging_events e
LEFT JOIN staging_songs s ON (e.song = s.title)
WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT e.userId as user_id, e.firstName as first_name, e.lastName as last_name, e.gender, e.level
FROM staging_events e
WHERE e.page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT s.song_id, s.title, s.artist_id, s.year, s.duration
FROM staging_songs s
JOIN staging_events e ON (e.song = s.title)
WHERE e.page='NextSong';
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT s.artist_id, s.artist_name as name, s.artist_location as location, s.artist_latitude as latitude, s.artist_longitude as longitude
FROM staging_songs s;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM staging_events WHERE page='NextSong')
SELECT DISTINCT ts as start_time,
extract(hour from ts),
extract(day from ts),
extract(week from ts),
extract(month from ts),
extract(year from ts),
extract(weekday from ts)
FROM temp_time;
""")


# QUALITY CHECK QUERIES

error_chk = ("""select count(*) from stl_load_errors;""")
staging_events_chk = ("""select count(*) from staging_events;""")
staging_songs_chk = ("""select count(*) from staging_songs;""")
songplays_chk = ("""select count(*) from songplays;""")
users_chk = ("""select count(*) from users;""")
songs_chk = ("""select count(*) from songs;""")
artists_chk = ("""select count(*) from artists;""")
time_chk = ("""select count(*) from time;""")


# EXPLORATION AND INSIGHTS QUERIES

top_10_most_played_songs = ("""
SELECT s.song_id, s.title, s.artist_id, COUNT(sp.song_id) AS num_plays
FROM songs s
JOIN songplays sp ON s.song_id = sp.song_id
GROUP BY s.song_id, s.title, s.artist_id
ORDER BY num_plays DESC
LIMIT 10;""")

top_10_songs_by_gender = ("""
SELECT s.song_id, s.title, u.gender, COUNT(sp.song_id) AS num_plays
FROM songs s
JOIN songplays sp ON s.song_id = sp.song_id
JOIN users u ON sp.user_id = u.user_id
GROUP BY s.song_id, s.title, u.gender
ORDER BY u.gender, num_plays DESC
LIMIT 10;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


# ADDITIONAL / OPTIONAL QUERIES

quality_queries = [error_chk, staging_events_chk, staging_songs_chk, songplays_chk, users_chk, songs_chk, artists_chk, time_chk]

exploration_queries = {"top_10_most_played_songs":top_10_most_played_songs, "top_10_songs_by_gender": top_10_songs_by_gender}
