import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from sql_queries import songplay_table_insert, time_table_insert, song_select, song_table_insert, artist_table_insert, select_song_play_events,user_table_insert,select_song_play_events

import psycopg2.extras as extras
import pandas as pd
import numpy
from psycopg2.extensions import register_adapter, AsIs

#register PostgreSQL adapters for numpy.int64 and numpy.float64
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

''' 
Copy data from S3 buckets to staging tables
'''
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

''' 
insert data in to artist and song tables from staging song table.
'''
def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        
''' 
    This is my main processing function. I am reading data from staging event table in to a data frame and parse the data frame for time and user records
    These records are converted to tuples and are inserted in to user and time tables. Finally we iterate through Data Frame to get song id and artist id 
    from songs and artist table based on title and artist name in event data and the resulting data is inserted in to songplay table
    As Redshift does not support upserts, there is a need to insert only latest user records. So process the log file by latest timestamp and keep track of 
    user ids by inserting them in a Hash Set. Similarly processing of time dimension records, we only insert one record by keeping tracking of this timestamp 
    in a HashSet
Input: Cursor and Conn objects
Output: None
'''
def process_staging_tables(cur, conn):
    
    #read the log data from staging events table
    
    df = pd.read_sql(select_song_play_events, conn)    
    
    #filter by NextSong action
    df = df[df['page'] == 'NextSong'] 
    
    #convert timestamp to series'''
    df1 = pd.Series(pd.to_datetime(df['ts'], unit='ms'))    
    
    #create user dictionary '''
    user_dict = {"user_id":df['userid'], "first_name":df['firstname'], "last_name":df['lastname'], "gender":df['gender'], "level":df['level']}
    
    #load user data from our dataframe
    user_df = pd.DataFrame.from_dict(user_dict)
    
    #remove those records which have no user_id
    
    user_df = user_df[~user_df['user_id'].isnull()]
    
    #Hashset to discard user_ids which are already inserted
    
    current_users = set()
        
    for i,row in user_df.iterrows():
        if(row['user_id'] not in current_users):
            cur.execute(user_table_insert, row)
        current_users.add(row['user_id'])       
    conn.commit(); 
    
    
    # convert timestamp column to datetime
    t = [pd.to_datetime(df['ts'], unit='ms'), df1.dt.hour, df1.dt.day, df1.dt.week, df1.dt.month, df1.dt.year, df1.dt.weekday]
    
    # insert time data records
    time_data = (df['ts'], df1.dt.hour, df1.dt.day, df1.dt.week, df1.dt.month, df1.dt.year, df1.dt.weekday)
    column_labels = {"start_time":df['ts'], "hour":t[1], "day":t[2], "week":t[3], "month":t[4], "year":t[5], "weekday":t[6]}
    time_df = pd.DataFrame.from_dict(column_labels)
    #hashset to track inserted time records.
    time_data = set()

    for i, row in time_df.iterrows():
        if(row['start_time'] not in time_data):
            cur.execute(time_table_insert, row)
            time_data.add(row['start_time'])
    conn.commit()   
    print("All done. All done")
    
 
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    register_adapter(numpy.float64, addapt_numpy_float64)
    register_adapter(numpy.int64, addapt_numpy_int64)

    process_staging_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()