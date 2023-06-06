import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    """
    - read the json file of song 'line by line'
    """
    df = pd.read_json(filepath, lines=True)

    # insert song record
    """
    - definition the columns name in song table.
    - insert the values on the table.
    """
    
    song_columns = ['song_id','title','artist_id','year','duration']
    song_data = df[song_columns].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    """ 
    - definition the columns name in artist table.
    - insert the values on the table.
    """
    artist_columns = ['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    artist_data = df[artist_columns].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    """
    - read the json file of log 'line by line'.
    - convert 'ts' column to datatime.
    - filter 'page' column by (NextSong).
    - divide 'ts' column to (hour,day,week,month,year,weekday).
    - insert the values on the new columns.
    """
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'],unit='ms')
    
    # insert time data records
    time_data = [[t,t.dt.hour,t.dt.day,t.dt.weekofyear,t.dt.month,t.dt.year,t.dt.weekday]]
    column_labels = ['start_time','hour','day','week','month','year','weekday']
    time_df = pd.DataFrame.from_dict({c:t for c, t in zip(column_labels,time_data)})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    """
    - definition the columns name in user table.
    """
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    """
    - insert the values on the user table.
    """
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        """
        - insert the values records for each column in song table.
        """
        songplay_data = [row.ts, row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    -This function is responsible for listing the files in a directory, and then excuting the ingest process for each file          according to the function that performs the transformation to save it to the database.
    -cur: the cursor object.
    -conn: connection to the database.
    -filepath: log data or song data file path.
    -func: function that transforms the data and inserts it into the database.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()