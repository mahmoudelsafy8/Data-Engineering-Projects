import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop =  "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop =       "DROP TABLE IF EXISTS songplay"
user_table_drop =           "DROP TABLE IF EXISTS user"
song_table_drop =           "DROP TABLE IF EXISTS song"
artist_table_drop =         "DROP TABLE IF EXISTS artist"
time_table_drop =           "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (
                                                                             artist          TEXT,
                                                                             auth            TEXT,
                                                                             firstname       TEXT,
                                                                             gender          CHAR(1),
                                                                             itemInSession   INT,
                                                                             lastname        TEXT,
                                                                             length          FLOAT,
                                                                             level           TEXT,
                                                                             location        TEXT,
                                                                             method          TEXT,
                                                                             page            TEXT,
                                                                             registration    FLOAT,
                                                                             sessioId        INT,
                                                                             song            TEXT,
                                                                             status          INT,
                                                                             ts              BIGINT,
                                                                             userAgent       TEXT,
                                                                             userId          INT NOT NULL
);
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_song (
                                                                           num_song          INT,
                                                                           artist            VARCHAR,
                                                                           artist_latitude   FLOAT,
                                                                           artist_longitude  FLOAT,
                                                                           artist_location   TEXT,
                                                                           artist_name       TEXT,
                                                                           song_id           VARCHAR,
                                                                           title             TEXT,
                                                                           duration          FLOAT,
                                                                           year              INT
);
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplay (
                                                                  songplay_id              INT IDENTITY(0,1) PRIMARY KEY,
                                                                  start_time               BIGINT,
                                                                  user_id                  INT NOT NULL,
                                                                  level                    TEXT,
                                                                  song_id                  VARCHAR,
                                                                  artist_id                VARCHAR NOT NULL,
                                                                  session_id               INT,
                                                                  location                 TEXT,
                                                                  user_agent               TEXT
);
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS user (
                                                          user_id                          INT NOT NULL PRIMARY KEY,
                                                          first_name                       TEXT,
                                                          last_name                        TEXT,
                                                          gender                           CHAR(1),
                                                          level                            TEXT
);
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS song (
                                                          song_id                          VARCHAR NOT NULL PRIMARY KEY,
                                                          title                            TEXT,
                                                          artist_id                        VARCHAR NOT NULL,
                                                          year                             INT,
                                                          duration                         FLOAT
);
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artist (
                                                              artist_id                    VARCHAR NOT NULL PRIMARY KEY,
                                                              name                         TEXT,
                                                              location                     TEXT,
                                                              latitude                     FLOAT,
                                                              longitude                    FLOAT
);
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
                                                          start_time                       TIMESTAMP NOT NULL PRIMARY KEY,
                                                          hour                             INT,
                                                          day                              INT,
                                                          week                             INT,
                                                          month                            INT,
                                                          year                             INT,
                                                          weekday                          INT
);
""")

# STAGING TABLES

staging_events_copy = (""" COPY staging_events
                           FROM {}
                           CREDENTIALS 'aws_iam_role= {}'
                           REGION 'us-west-2'
                           FORMAT AS JSON '{}'
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE','ARN'),
            config.get('S3','LOG_JSONPATH'))

staging_songs_copy = (""" COPY staging_songs
                          FROM '{}'
                          CREDENTIALS 'aws_iam_role= {}'
                          REGION 'us-west-2'
                          FORMAT AS JSON '{}'
                          
""").format(config.get('S3','SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplay (songplay_id,start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
                             SELECT DISTINCT
                                            ss.songplay_id,
                                            TO_TIMESTAMP(e.ts::TIMESTAMP/1000) AS start_time,
                                            se.user_id,
                                            se.level,
                                            ss.song_id,
                                            ss.artist_id,
                                            se.session_id,
                                            se.location,
                                            se.user_agent
                                    
                             FROM           staging_events se,
                             JOIN           staging_songs  ss
                             ON             se.song = ss.title
                             WHERE          se.page = 'NextSong'
                             AND            se.artist = ss.artist_name
                             AND            se.length = ss.duration;

""")

user_table_insert = (""" INSERT INTO user (user_id,first_name,last_name,gender,level)
                         SELECT DISTINCT 
                                        user_id,
                                        first_name,
                                        last_name,
                                        gender,
                                        level
                          FROM          staging_events
                          WHERE         page = 'NextSong';
""")

song_table_insert = ("""INSERT INTO song (song_id,title,artist_id,year,duration)
                        SELECT DISTINCT
                                       song_id,
                                       title,
                                       artist_id,
                                       year,
                                       duration
                         FROM          staging_songs;
""")

artist_table_insert = (""" INSERT INTO artist (artist_id,name,location,latitude,longitude)
                           SELECT DISTINCT
                                          artist_id,
                                          name,
                                          location,latitude,
                                          longitude
                            FROM          staging_songs;              
""")

time_table_insert = (""" INSERT INTO time (start_time,hour,day,week,month,year,weekday)
                         SELECT DISTINCT
                                        TO_TIMESTAMP(ts::TIMESTAMP/1000)                      AS start_time,
                                        EXTRACT(HOUR FROM TO_TIMESTAMP(ts::TIMESTAMP/1000))   AS hour,
                                        EXTRACT(DAY FROM TO_TIMESTAMP(ts::TIMESTAMP/1000))    AS day,
                                        EXTRACT(WEEK FROM TIMESTAMP(ts::TIMESTAMP/1000))      AS week,
                                        EXTRACT(MONTH FROM TIMESTAMP(ts::TIMESTAMP/1000))     AS month,
                                        EXTRACT(YEAR FROM TIMESTAMP(ts::TIMESTAMP/1000))      AS year,
                                        EXTRACT(WEEKDAY FROM TIMESTAMP(ts::TIMESTAMP/1000))   As weekday
                          FROM          staging_events
                          WHERE         page = 'NextSong';              
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
