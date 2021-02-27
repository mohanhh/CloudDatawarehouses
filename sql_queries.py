import configparser


# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
# DROP TABLES

staging_events_table_drop = "Drop table if exists staging_events_table"
staging_songs_table_drop = "Drop table if exists staging_songs_table"
songplay_table_drop = "Drop table if exists songplays"
user_table_drop = "drop table if exists users cascade"
song_table_drop = "drop table if exists songs cascade"
artist_table_drop = "drop table if exists artists cascade"
time_table_drop = "drop table if exists time"

# CREATE TABLES

user_table_create = """create table if not exists users 
    (user_id bigint NOT NULL PRIMARY KEY distkey, 
    first_name varchar(256), 
    last_name varchar(256) sortkey, 
    gender varchar(8), 
    level varchar(64)) diststyle KEY"""

song_table_create = """create table if not exists songs 
    (song_id varchar(256) NOT NULL PRIMARY KEY distkey, 
    title varchar(256), artist_id varchar(256) NOT NULL, 
    year int sortkey, 
    duration numeric(30, 10), 
    foreign key(artist_id) references artists(artist_id)) 
    diststyle KEY"""

# Artist table
artist_table_create = """create table if not exists artists 
    (artist_id varchar(256) NOT NULL PRIMARY KEY, 
    name varchar(256), 
    location varchar(256) sortkey, 
    latitude double precision, 
    longitude double precision) 
    diststyle ALL"""

time_table_create = """create table if not exists time 
    (start_time bigint PRIMARY KEY NOT NULL distkey sortkey, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int) diststyle KEY"""

# Songplay fact table
songplay_table_create = """create table if not exists songplays 
    (songplay_id int IDENTITY(0,1) PRIMARY KEY distkey, 
    start_time bigint NOT NULL sortkey, 
    user_id bigint NOT NULL , 
    level VARCHAR(64), 
    song_id varchar(256) NOT NULL, 
    artist_id varchar(256) NOT NULL, 
    session_id VARCHAR(256), 
    location VARCHAR(256), 
    user_agent VARCHAR(256),
    foreign key(user_id)  references users(user_id), 
    foreign key (song_id) references songs(song_id), 
    foreign key(artist_id) references artists(artist_id)) diststyle KEY"""

# STAGING TABLES
staging_songs_table_create = ("""create table if not exists staging_songs_table  
    (num_songs int, 
    artist_id varchar(256), 
    artist_latitude double precision, 
    artist_longitude double precision, 
    artist_location varchar(256), 
    artist_name varchar(256), 
    song_id varchar(256), 
    title varchar(256), 
    duration numeric (30, 10), 
    year int) diststyle ALL
""")
staging_events_table_create= ("""
    create table if not exists staging_events_table 
    (artist varchar(256), 
    auth varchar(256), 
    firstName varchar(256), 
    gender varchar(10), 
    itemInSession int, 
    lastName varchar(256), 
    length numeric(30, 3), 
    level varchar(64), 
    location varchar(100), 
    method varchar(10), 
    page varchar(32), 
    registration decimal(30, 3), 
    sessionId bigint, 
    song varchar(256), 
    status int, 
    ts bigint sortkey, 
    userAgent varchar(256), 
    userId bigint) diststyle auto
""") 

staging_events_copy = ("""copy staging_events_table from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json 's3://udacity-dend/log_json_path.json';
""".format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH')))

staging_songs_copy = ("""copy staging_songs_table  from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json 'auto'
""".format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN')))

# FINAL TABLES

songplay_table_insert = ("""insert 
    into
        songplays
        (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
            (select
                a1.ts,
                a1.userId,
                a1.level,
                a2.song_id,
                a2.artist_id,
                a1.sessionId,
                a1.location,
                a1.useragent 
            from
                staging_events_table a1 
            join
                staging_songs_table a2
                    on a1.artist = a2.artist_name 
                    and a2.title = a1.song 
                    and a2.duration = a1.length 
                    and a1.page='NextSong'
            );""") 


user_table_insert = ("""insert into users 
    (user_id, 
    first_name, 
    last_name, 
    gender, 
    level) 
    values (%s, %s, %s, %s, %s)
    """)

song_table_insert = ("""insert into songs 
    (song_id, title, artist_id, duration, year) 
    (select  distinct song_id, title, artist_id, duration, year from staging_songs_table);""")

artist_table_insert = ("""insert into artists (artist_id, name, location, latitude, longitude) (select  distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude from staging_songs_table);""")



time_table_insert = ("""insert into time 
    (start_time, hour, day, week, month, year, weekday) 
    values (%s, %s, %s, %s, %s, %s, %s)""")

# Select query to read everything from select song play staging table
select_song_play_events = ("""select artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration , sessionId, song, status, ts, userAgent, userId from staging_events_table order by ts desc""")

# FIND SONGS

song_select = ("""select s1.song_id as songid, a1.artist_id as artistid from songs s1 join artists a1 on s1.artist_id=a1.artist_id where s1.title=%s and a1.name=%s and s1.duration=%s""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,user_table_create, artist_table_create, song_table_create,  time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [song_table_insert, artist_table_insert, songplay_table_insert]
