# Sparkify Database
## Introduction:
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. So in this Sparkify database we are loading two source files song data and log data placed in S3 bucket into one fact table and four dimension tables in S3 target bucket in different folders using ETL written pyspark. So that analytics team can easily access normalized data using tool like AWS athena to get insights from that data which will help business to take decisions.

## Source Dataset:
### Song Dataset: 
Each file is in JSON format and contains metadata about a song and the artist of that song.The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.
Bucket Path : s3://udacity-dend/song_data
File Examples: 
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json

### Log Dataset: 
Dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations. This file partitioned by year and month.
Bucket Path : s3://udacity-dend/log_data
File Examples:
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json

## Database Schema:
### Fact Table:
Name : songplays 
In this table we loading log data associated with song plays and it contains below columns.
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables:
Name : users 
In this table we are loading data related to user which is getting loaded from log data and it contains below columns.
user_id, first_name, last_name, gender, level

Name : songs
In this table we are loading data related to song which is getting loaded from song data and it contains below columns.
song_id, title, artist_id, year, duration

Name : artists
In this table we are loading data related to artist which is getting loaded from song data and it contains below columns.
artist_id, name, location, latitude, longitude

Name : time 
In this table we are loading data related to timestamp and all columns in this table are extracted from one column timestamp of log data file and it contains below columns.
start_time, hour, day, week, month, year, weekday

## Project Code Files:
### dl.cfg:
This file contains aws credentials we are required to access S3 bucket.

### etl.py:
This script code is written to read and processes song and log data files from s3 using pyspark and load them in into five different s3 parquet target folders which will act as five tables and can be analyze using aws athena.

## Commands:
### Command to install required packages:
#### to install pyspark package:
pip install pyspark
### Command to read S3 data files and load data into target tables:
python etl.py