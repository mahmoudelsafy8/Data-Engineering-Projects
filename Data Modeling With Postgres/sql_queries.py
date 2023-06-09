# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Create database tables and definition the data type for each columns values and primary key

#Creatw songplay table
songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (
songplay_id    SERIAL PRIMARY KEY,
start_time     TIMESTAMP NOT NULL ,
user_id        INT NOT NULL,
level          VARCHAR NOT NULL,
song_id        VARCHAR,
artist_id      VARCHAR,
session_id     INT NOT NULL,
location       VARCHAR NOT NULL,
user_agent     VARCHAR NOT NULL
)""")

# Create user table
user_table_create = (""" CREATE TABLE IF NOT EXISTS users (
user_id        INT PRIMARY KEY,
first_name     VARCHAR NOT NULL,
last_name      VARCHAR NOT NULL,
gender         CHAR(1) NOT NULL,
level          VARCHAR NOT NULL
)""")

# Create song_table
song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (
song_id        VARCHAR PRIMARY KEY,
title          VARCHAR NOT NULL,
artist_id      VARCHAR NOT NULL,
year           INT NOT NULL,
duration       FLOAT NOT NULL
)""")

# Creat artist table
artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
artist_id      VARCHAR PRIMARY KEY,
name           VARCHAR NOT NULL,
location       VARCHAR NOT NULL,
latitude       FLOAT NOT NULL,
longitude      FLOAT NOT NULL
)""")

# Create time table
time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
start_time     TIMESTAMP PRIMARY KEY,
hour           INT NOT NULL,
day            INT NOT NULL,
week           INT NOT NULL,
month          INT NOT NULL,
year           INT NOT NULL,
weekday        INT NOT NULL
)""")

# INSERT RECORDS

# Insert the records for each tables in the database using Insert Into 'name of table' (columns name) Values (put '%s' for each coulmns)

songplay_table_insert = ("""INSERT INTO songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT DO NOTHING
""")

user_table_insert = ("""INSERT INTO users (user_id,first_name,last_name,gender,level) 
                        VALUES(%s,%s,%s,%s,%s) 
                        ON CONFLICT (user_id) DO UPDATE SET level = excluded.level
""")

song_table_insert = ("""INSERT INTO songs(song_id,title,artist_id,year,duration)
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING
""")

artist_table_insert = ("""INSERT INTO artists (artist_id,name,location,latitude,longitude)
                          VALUES(%s,%s,%s,%s,%s)
                          ON CONFLICT (artist_id) DO NOTHING
""")


time_table_insert = ("""INSERT INTO time (start_time,hour,day,week,month,year,weekday)
                        VALUES(%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING
""")

# FIND SONGS
# joins both tables togather (songs & artists) on primary key 'artist_id' with condition Where

song_select = ("""SELECT songs.song_id,artists.artist_id 
                  FROM songs 
                  JOIN artists 
                  ON songs.artist_id = artists.artist_id 
                  WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]