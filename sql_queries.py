import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists stagingevents"
staging_songs_table_drop = "drop table if exists stagingsongs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE if not exists stagingevents 
                                    (artist        text,         
                                    auth          text,
                                    firstName     text,
                                    gender        text,
                                    itemInSession integer,
                                    lastName      text,
                                    length        numeric,
                                    level         text,
                                    location      text,
                                    method        text,
                                    page          text,
                                    registration  numeric,
                                    sessionId     integer,
                                    song          text,
                                    status        integer,
                                    ts            bigint,
                                    userAgent     text,
                                    userId        integer)
""")

staging_songs_table_create = ("""CREATE TABLE if not exists stagingsongs 
                                    (num_songs         integer, 
                                    artist_id         text,
                                    artist_latitude   text,
                                    artist_longitude  text,
                                    artist_location   text,
                                    artist_name       text ,
                                    song_id           text,
                                    title             text,
                                    duration          numeric,
                                    year              integer)
""")


songplay_table_create = ("""create table if not exists songplays  
                            (songplay_id bigint IDENTITY(0,1) NOT NULL,
                             start_time bigint NOT NULL REFERENCES time, 
                             user_id integer  NOT NULL REFERENCES users, 
                             level text, 
                             song_id text  REFERENCES songs, 
                             artist_id text   REFERENCES artists, 
                             session_id integer , 
                             location text, 
                             user_agent text,
                             PRIMARY KEY (songplay_id))
""")

user_table_create = ("""create table if not exists users
                        (user_id integer NOT NULL,
                         first_name text, 
                         last_name text, 
                         gender text, 
                         level text,
                         PRIMARY KEY (user_id) )
""")

song_table_create = ("""create table if not exists songs 
                        (song_id text NOT NULL, 
                         title text, 
                         artist_id text, 
                         year numeric, 
                         duration numeric,
                         PRIMARY KEY (song_id))
""")

artist_table_create = ("""create table if not exists artists 
                          (artist_id text NOT NULL, 
                           name text, 
                           location text, 
                           latitude text, 
                           longitude text,
                           PRIMARY KEY (artist_id))
""")

time_table_create = ("""create table if not exists time 
                        (start_time bigint NOT NULL, 
                         hour integer, 
                         day integer, 
                         week integer, 
                         month integer, 
                         year integer, 
                         weekday integer,
                         PRIMARY KEY (start_time))
""")

# STAGING TABLES

staging_events_copy = ("""copy stagingevents 
                            from {} 
                            region 'us-west-2' 
                            iam_role {} 
                            json {}
""").format(config['S3']['LOG_DATA'], 
            config['IAM_ROLE']['ARN'],
            config['S3']['LOG_JSONPATH']
           )


staging_songs_copy = ("""copy stagingsongs 
                            from {}
                            region 'us-west-2'
                            iam_role {}
                            json 'auto'
""").format(config['S3']['SONG_DATA'], 
            config['IAM_ROLE']['ARN']
            )

# FINAL TABLES

songplay_table_insert = ("""insert into songplays 
                        (
                        start_time,
                        user_id,
                        level,
                        song_id,
                        artist_id,
                        session_id,
                        location,
                        user_agent)
                        select  ts, userid, level, song_id, artist_id, sessionId, artist_location, userAgent
                        FROM stagingevents se, stagingsongs ss
                        WHERE 
                        (se.page = 'NextSong'
                        AND se.song = ss.title
                        AND se.artist = ss.artist_name
                        AND se.length = ss.duration )
""")

user_table_insert = ("""insert into users 
                        (user_id,
                        first_name,
                        last_name,
                        gender, 
                        level)
                         select distinct se.userId, se.firstName, se.lastName, se.gender, se.level 
                         from stagingevents se
                         WHERE page = 'NextSong'
""")

song_table_insert = ("""insert into songs 
                        (song_id,
                        title,
                        artist_id,
                        year,
                        duration)
                        select ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
                        from stagingsongs ss
  
""")

artist_table_insert = ("""insert into artists 
                         (artist_id,
                         name,
                         location,
                         latitude,
                         longitude) 
                          select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
                          from stagingsongs ss
""")


time_table_insert = ("""insert into time 
                        (start_time,
                        hour,
                        day,
                        week,
                        month,
                        year,
                        weekday)
                        select 
                            distinct ts
                            ,EXTRACT(HOUR FROM ts_time) As t_hour
                            ,EXTRACT(DAY FROM ts_time) As t_day
                            ,EXTRACT(WEEK FROM ts_time) As t_week
                            ,EXTRACT(MONTH FROM ts_time) As t_month
                            ,EXTRACT(YEAR FROM ts_time) As t_year
                            ,EXTRACT(DOW FROM ts_time) As t_weekday
                        from
                        (SELECT distinct ts, timestamp 'epoch'::date + ts/1000 * interval '1 second' as ts_time
                         FROM stagingevents se)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [ staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
