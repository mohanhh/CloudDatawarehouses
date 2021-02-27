# Fact Table
## songplays - 
### Holds Records from log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
As the log file has only song's title and the artist name, song_id and artist_id are extracted 
by comparing song's title, it's duration in song database and artist name in artist database.
songplay_id is set as auto increment field in songplay table. As this is the main fact table, it is
distributed across all the nodes.

# Dimension Tables
## users - 
### users in the app. Users created by extracting user_id, first_name, last_name, gender, level from log files.
Added constraint to overwrite user's first name, last name, gender and level when we encounter the user id again in log 
file. Also added constraint to make user_id not null. As Redshift does not support Upserts, start by sorting the song_event table
using timestamp (ts) in descending order and extracting user information from last. This way if any user updated the subscription or name
we will always have latest information. User table could be large and hence is distributed across all the nodes. 

## songs - songs in music database
## song_id, title, artist_id, year, duration
This table is created from song data set. Added constraint to make song_id Primary key which means it is unique and not 
null. Song table is distributed across all the nodes
 
## artists - artists in music database
## artist_id, name, location, latitude, longitude
Artist table is also created from song data set. Added constraint to make artist_id Primary Key which means we assume 
artist id is unique and not null. Artist table is copied to all the nodes as this table should be comparitively smaller
table and as it is frequently joined with songs table, joins will be faster.

## time - timestamps of records in songplays broken down into specific units
### time_id, start_time, hour, day, week, month, year, weekday
time dimension shows time user played a particular song. The timestamp from log file is processed to store
start_time, hour, day, week, month, year and weekday when user played the song. timestamp is being used as Primary key as 
other entries in the table are calculated from this value.

Taken together these tables should allow Data Engineering team at Sparkify to analyze user data. songplay contains 
individual records of what song user's played at what time. It also will support aggregate queries like 
how many songs user played on the platform, which are popular songs and who are popular artists on the 
platform, how many songs a paid customer played and which are popular browsers for users on Sparkify.  songplays table along
with time table also identifies number of users in the system which can help in scaling the platform.  

# ETL Process
ETL Process currently parses song files first. This process extracts song_id, song_title, duration, year and artist id. 
Same data is used to populate artist table by extracting artist name, artist id, artist location, location's latitude and longitude.

Once song and artist tables are populated, log data is processed and each line of log is used to populate user and time dimension tables.
songplay table is populated by comparing song's title, duration and artist names to the corrsponding values in song and artist tables. 

## How to run the code
Use runAll.ipnb notebook to run the ETL process. It first runs create_tables.py and then runs etl.py