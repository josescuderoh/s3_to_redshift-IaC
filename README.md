# Project Description

## Business need
The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Implementation

An ETL process to create a Redshift data warehouse with tables designed to optimize queries on song play analysis has been created and tested using the queries provided by the team. 

The ETL process takes multiple JSON files from song data and logs on user activity and copies it in parallel from S3 buckets, transforming it to create and populate the a star schema. In order to perform a transactional analysis of songplays, the fact and dimension tables for a star schema have been implemented and released using Python and SQL.

### Staging tables

Two staging tables have been created as target for the copy command in order to dump the raw JSON data into the DWH:
- `staging_events`: compiles all the events available for processing.
- `staging_songs`: compiles all the songs and artist information that users can listen to.

### Schema

Based on the requirements of the analytics team, the relation `songplays` is proposed as the fact table which describes the events in which a specific user interacted with a song, and other metadata for such event. The dimensions table provide additional information about the users, songs, artists and time metadata.

#### Fact Table
- `songplays` - records in log data associated with song plays i.e. records with page NextSong. Attributes: *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

#### Dimension Tables
- `users` - users in the app. Attributes: *user_id, first_name, last_name, gender, level*
- `songs` - songs in music database. Attributes: *song_id, title, artist_id, year, duration*
- `artists` - artists in music database. Attributes: *artist_id, name, location, latitude, longitude*
- `time` - timestamps of records in songplays broken down into specific units. Attributes: *start_time, hour, day, week, month, year, weekday*

### Execution

The following bash commands must be executed from the root directory in order to reset/start populating the database with the files available in S3:

```bash
python create_tables.py #To create/reset the database
python etl.py # To run the ETL process
```

Details about the development of the ETL process are compiled in `etl.ipynb`. All the queries are imported from the `sql_queries.py` file containing multi-line strings to interact with the database.

Moreover, a file `manage_cluster.py` has been created to handle creation and deletion of resources via Infrastructure as Code (IaC), using credentials provided in `aws.cfg`.

### Access

The below the information to find the database can be found in `dwh.cfg` which is automatically populated once the functions in `manage_cluster.py` are executed inside a Python shell:

- host
- dbname
- user 
- password
- port
- IAM arn

```sql
SELECT * FROM songplays LIMIT 5;
```

To get a sample of the contents of songplays.

```sql
select distinct song_id, artist_id from songplays;
```

This command will show the matching `song_id` and `artist_id` that are available when the `songplays` fact table is populated.