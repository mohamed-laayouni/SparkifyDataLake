### Sparkify database set up

## Intro
As our analytical team, in Sparkify, wants to analyze users song listening activity, we will be setting up a datawarehouse optimized for this analytical purpose which summarizes all the information users provide when using our platform, this includes their song activity, their client details, the timeframe of their song playbacks, etc...

## Datawarehouse design

I chose to set up the Datawarehouse "Sparkifydatawareshouse" on Redshift to better accomodate the data collected on S3. Using a star schema, to better address the analytical nature of the queries the Sparkify analytical team will be running on it, and seamlessly building a pipeline that can automatically extract data from S3 and load into the tables in Redshift.
At the center of the star, is our fact table: songplays, linked to our dimension tables with 4 foreign keys (song_id, artist_id, sessions_id, user_id). the dimension tables I set up are: Songs, Artists, Users and Times, each covering: Songs information, artists information, users information and playtime information (respectively).

## Files in this repo
This repo contains 4 files: etl.py, sql_queries.py, create_tables.py, and dwh.cfg
> The sql_queries.py sets up the table design in our databse, defines each tables components and their relationships. It also extracts and copies the data from s3 directly into our staging tables and then populates the main tables that we will be using for our analytical purposes.
> The create_tables.py executes the sql queries in sql_queries.py, it establishes a connection to Postgre using Psycopg2 and sets up the database on it. It also contains some table dropping queries to ensure that multiple runs don't create duplicated tables.
> The etl.py extracts our data from our log and song info files, wrangles it to prepare it for insertion in our tables and then populates our tables with all the necessary information.
> dwh.cfg contains information about the Redshift database in AWS, the IAM role necessary to run it by an AWS user, and the S3 links for copying the data in json format to a sql table.

## How to run the files.
After creating the redshift table in AWS and getting the necessary endpoints as well as the IAM role ARN, 
You can, using python shell, run create_tables.py first to create the tables in Redshift and load the data from s3. You can then run the etl pipeline with etl.py to extract the data and insert it into the databse. 
