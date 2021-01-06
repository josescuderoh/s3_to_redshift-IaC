import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
create table staging_events (
artist varchar,
auth varchar,
firstName varchar,
gender varchar(2),
itemInSession int,
lastName varchar,
length numeric,
level varchar(10),
location varchar,
method varchar(4),
page varchar,
registration bigint,
sessionId int,
song varchar,
status int,
ts bigint,
userAgent varchar,
userId int
)
""")

staging_songs_table_create = ("""
create table staging_songs (
num_songs int, 
artist_id varchar, 
artist_latitude numeric, 
artist_longitude numeric,
artist_location varchar,
artist_name varchar, 
song_id varchar,
title varchar,
duration numeric,
year int
)
""")

songplay_table_create = ("""
create table songplays (
songplay_id varchar primary key, 
start_time bigint not null references time(start_time) sortkey, 
user_id int not null references users(user_id), 
level varchar, 
song_id varchar not null references songs(song_id) distkey, 
artist_id varchar not null references artists(artist_id), 
session_id int, 
location varchar, 
user_agent varchar
)
""")

user_table_create = ("""
create table users (
user_id int primary key sortkey, 
first_name varchar, 
last_name varchar, 
gender varchar, 
level varchar
)
""")

song_table_create = ("""
create table songs(
song_id varchar primary key sortkey distkey, 
title varchar, 
artist_id varchar not null references artists(artist_id), 
year int, 
duration numeric
)
""")

artist_table_create = ("""
create table artists(
artist_id varchar primary key sortkey, 
name varchar, 
location varchar, 
latitude numeric, 
longitude numeric
)
""")

time_table_create = ("""
create table time(
start_time bigint primary key sortkey, 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday int
)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from '{}'
credentials 'aws_iam_role={}'
format as json '{}';
""").format(
    config.get('S3', 'log_data'),
    config.get('IAM_ROLE', 'arn'), 
    config.get('S3', 'log_jsonpath')
)

staging_songs_copy = ("""
copy staging_songs from '{}'
credentials 'aws_iam_role={}'
format as json 'auto';
""").format(
    config.get('S3', 'song_data'),
    config.get('IAM_ROLE', 'arn'))

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays
select st.ts || st.userId || st.sessionId,
st.ts, st.userId, st.level, s.song_id, a.artist_id, st.sessionId, st.location, st.userAgent
from staging_events st
join songs s on s.title = st.song
join artists a on a.name = st.artist
where st.page='NextSong';
""")

user_table_insert = ("""

-- Create a staging table and populate it with updated and unique rows

create temp table stageusers as
select distinct userId, firstName, lastName, gender, level
from staging_events
where userId is not null
and page='NextSong';

-- Start a new transaction
begin transaction;

delete from users
using stageusers su
where users.user_id = su.userId;

insert into users
select *
from stageusers;

-- End transaction and commit
end transaction;

-- Drop the staging table
drop table stageusers;
""")

song_table_insert = ("""
insert into songs 
select song_id, title, artist_id, year, duration
from staging_songs
""")

artist_table_insert = ("""
insert into artists 
select artist_id, artist_name, artist_location, artist_latitude, artist_longitude
from staging_songs
""")

time_table_insert = ("""
-- Create a staging table and populate it with updated and unique rows

create temp table stagetime as
select distinct
ts,
extract('h' from timestamp 'epoch' + ts/1000 * interval '1 second') as hour,
extract('d' from timestamp 'epoch' + ts/1000 * interval '1 second') as day,
extract('w' from timestamp 'epoch' + ts/1000 * interval '1 second') as week,
extract('mon' from timestamp 'epoch' + ts/1000 * interval '1 second') as month,
extract('y' from timestamp 'epoch' + ts/1000 * interval '1 second') as year,
extract('weekday' from timestamp 'epoch' + ts/1000 * interval '1 second') as weekday
from staging_events
where page='NextSong';

-- Start a new transaction
begin transaction;

delete from time
using stagetime st
where time.start_time = st.ts;

insert into time
select *
from stagetime;

-- End transaction and commit
end transaction;

-- Drop the staging table
drop table stagetime;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, artist_table_insert, song_table_insert, time_table_insert, songplay_table_insert]
