# AWS DATA WAREHOUSE PROJECT (Udacity DEND)

## PROBLEM OVERVIEW
Music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## SUMMARY OF OBJECTIVES AND ACHIEVEMENTS
Built an ETL pipeline that extracts Sparkify's data from S3, stages them in Redshift, then transforms the data into a set of fact and dimensional tables to faciliate exploration and analytics.

## PYTHON SCRIPTS
Udacity-provided template files/scripts were used as a starting point for the project:
* dwh.cfg - config file houses S3 file locations, RedShift cluster info and IAM role for accessing S3.
* sql_queries.py - script containing queries for dropping, creating, copying, inserting, and quality checking tables, as well as two optional example exploration queries.
* create_tables.py - script for executing sql queries for dropping and creating the sql tables in RedShift.
* etl.py - script for executing sql queries for copying/staging (from S3), inserting, quality checking, and exploring in RedShift. 

### How to Run
* With all files in the same directory
* Using terminal navigate to directory and create/activate virtual env with required libraries.
* Create RedShift cluster (preferrably us-west-2 for quickest S3 copy)
* Update the dwh.cfg file with your IAM Role and RedShift cluster info.
* Update the sql_queries COPY queries with your IAM_ROLE string.
* With the cluster running, run "create_tables.py"
* Then run "etl.py"

Note: using 8 x dc2.large nodes with a us-west-2 region cluster, the scripts should run completely within 3-5mins.

## DATABASE SCHEMA AND ETL PIPELINE
### Database Star Schema
The Udacity-provided database schema was used / completed for the project. The schema was logical and fit the star schema design:
* The fact table captured metric-like information - for example, an analyst could determine what songs are most played, or what artists are most popular.
* While the dimension tables capture related details or context - for example, an analyst could drill down and determine where the most popular artists are located, or when during the day is the most popular music streaming time.

General Notes:
* Primary keys (PKs) were created from the original data where id/key-like columns were present; otherwise, serial/auto-incrementing PKs were employed.
* "TEXT" datatypes were used for most string/text fields for simplicity.
* "FLOAT" was used for most decimal numeric data columns as it seemed sufficient (rather than using DOUBLE precision)
* NOT NULL condition was applied to columns were it made sense (e.g. artist name, id/key columns), but not everywhere to avoid being overly restrictive and losing data.
* WHERE page="NextSong" filtering condition was applied as required to filter out any non-songplay event records.

### ETL Pipeline
The ETL pipeline script was well organized and streamlined and so was basically adopted as-is with some minor additions noted below:
* Query execution time code snippets added.
* 2 additional functions added to run the quality-check and optional exploration queries.

## ADDITIONAL QUERIES
Two optional queries were provided as examples of what data analysts might find insightful.

### Queries
```
top_10_most_played_songs = ("""
SELECT s.song_id, s.title, s.artist_id, COUNT(sp.song_id) AS num_plays
FROM songs s
JOIN songplays sp ON s.song_id = sp.song_id
GROUP BY s.song_id, s.title, s.artist_id
ORDER BY num_plays DESC
LIMIT 10;""")
```

```
top_10_songs_by_gender = ("""
SELECT s.song_id, s.title, u.gender, COUNT(sp.song_id) AS num_plays
FROM songs s
JOIN songplays sp ON s.song_id = sp.song_id
JOIN users u ON sp.user_id = u.user_id
GROUP BY s.song_id, s.title, u.gender
ORDER BY u.gender, num_plays DESC
LIMIT 10;
""")
```
