# Udacity Data Engineering Nanodegree
# Project: Data Warehouse

This project is a mockup for a fictional music streaming service, Sparkify, which wants to analyze the data they've collected on user activity.  They're interested in an ETL pipeline that extracts their data from S3 and transforms it into a set of dimension and fact tables for their analytics team to investigate what songs their users are listening to.  

# Source Data

The source data is broken into two sets of JSON formatted files:

* log_data: log files partitioned by month and year and defined by a JSON schema file.  Each file contains multiple rows identifying songs played on the platform, including information such as artist name, user information, song information, and timestamps
* song_data: one song per file, describing artist and song information.  Files are spread across subdirectories in a specified location.

# Database Schema

The database is broken into the following tables:

Fact Table: 

* songplays - each record corresponds to a song play event, defined by `page==NextSong`, in the log_data.  Columns included in this table are: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, and user_agent

Dimension Tables:

* users - users in the app: user_id, first_name, last_name, gender, and level
* songs - songs in music database: song_id, title, artist_id, year, and duration
* artists - artists in music database: artist_id, name, location, latitude, and longitude
* time - timestamps of records in songplays broken down into specific units: start_time, hour, day, week, month, year, and weekday

The above schema prioritizes the goal of the workflow, analyzing song play analysis, by centering the schema on the songplay fact table.  This tries to minimize the JOIN statements required to analyze data related to song plays.  For example:

* Analyze the number of songplays for each user, or find which users have the most songplays (perhaps needed to understand what drives the most engagement, or similarly finding which users have lower engagement in order to target them with promotional material):
    * This requires no JOIN operations (unless additional user information is desired)
    ````
    SELECT user_id, count(songplay_id)
    FROM songplays
    GROUP BY user_id
    ORDER BY COUNT DESC
    LIMIT 5
    ````
    Result: 
    | user_id | count |
    |--------:|------:|
    |      49 |   772 |
    |      80 |   740 |
    |      97 |   595 |
    |      15 |   495 |
    |      44 |   439 |
    
* Analyze songplay data by time, in order to determine high/low usage periods (perhaps to schedule platform maintenance at off-peak hours, or to know when to provision more resources)
    * This requires no JOIN operations

* Analyze the engagement of users based on their subscription level and location
    * This requires no JOIN operations

While the above sorts of analyses are efficient, some workflows are less efficient than necessary given the current schema.  For example:

* Analyze whether male or female users listen to longer songs in the evening
    * (although a bit contrived) this requires JOINing the users, songs, time, and songplays table, adding considerable work to the query

If these workflows were important and frequent, it might make sense to add redundancy into the database or refactor some tables in order to improve these queries.  This would warrant a tradeoff analysis, however, as additional redundancy adds extra work at data load time, and refactoring could reduce analysis performance of the primary song play queries.

In order to produce the fact and dimension tables from the song and log data, two staging tables (staging_events and staging_songs) were used for loading data into Redshift before transformation.  These are not meant for analytical purposes and could be dropped after the ETL process was complete

# Table Optimization

This implementation does not use any distribution or sorting within the tables because: 

* the desired analytical workload was not specified.  Depending on the desired workload, setting a distribution strategy for the songplay table that is focused on the primary actions might be adventageous.  
* as we are examining a subset of the data, the distribution of the real data is unclear.  The balance between number of records of users vs artists appears to be different from what we'd expect in practice. 

Without foreknowledge about the workflow and data, Redshift's default optimization is a good option. 

If more detail on the potential data/workflow was known, a distribution strategy might be available that co-locates commonly joined information.  For example, assuming the songplay and user tables are both expected to be large and often joined, while the song, and artist tables are expected to be small (and edited less often), it could make sense to distribute the songplay and user tables by `key` with user_id and the song and artist tables by `all`.  Similarly, sorting could be defined based on expected workloads.

# Repository Contents

Included in the repository are:

* definitions of all sql queries required for table creation, table dropping, and common data insert/select
    * See `sql_queries.py`.  Intent is for this to be imported as utilities by other tools.  Not meant to run standalone
* table creation script which reinitializes the database, removing any existing tables and creating fresh ones
    * `python create_tables.py`
* the etl process, which takes raw data and puts that data into an **existing** db
    * `python etl.py`
* Metadata such as DB connection information, IAM role, and source data location in S3
    * See `dwh.cfg` for most details
  
# Run Instructions

To run the full ETL process, use:
````
python create_tables.py
python etl.py
````