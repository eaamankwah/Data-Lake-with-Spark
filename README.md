# Data-Lake-with-Spark

![rs_logo](https://github.com/eaamankwah/Data-Lake-with-Spark/blob/main/screenshots/sparklogo.png)

# Table of Contents
* Overview
* Project Dataset
* Star Database Schema Design
* * Fact Table
* * Dimension Tables
* Project Steps
* * Project Files
* * Running the Project
* * Build ETL Pipeline
* * Document Process
* References

## Overview
This project is fourth project of the Udacity Data Engineering Nanodegree. 

The object of this project is to construct a cloud-based data lake with Apache Spark, which includes building an ETL pipeline that extracts their data from AWS S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables for their analysis that generate data driven insight for the proposed client, Sparkify music business.

## Project Dataset

The dataset is a collected by and online startup platform called Sparkify who wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Sparkify wants to access the songs  that their users are listening to. The  data currently S3a, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

**Song Dataset**

The first dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. 

For example,  below are file paths to two files in this dataset.

* song_data/A/B/C/TRABCEI128F424C983.json 

* song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

**Log Dataset**

The second dataset consists of log files in JSON format generated by an [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

The log files in the dataset are partitioned by year and month. For example, here are file paths to two files in this dataset.

* log_data/2018/11/2018-11-12-events.json 

* log_data/2018/11/2018-11-13-events.json

## Star Database Schema Design

A star schema was used for this project and has the following benefits:

* Queries are simpler: because all of the data connects through the fact table the multiple dimension tables are treated as one large table of information, and that makes queries simpler and easier to perform.
* Easier business insights reporting: Star schemas simplify the process of pulling business reports like "what songs users are listening to".
* Better-performing queries: by removing the bottlenecks of a highly normalized schema, query speed increases, and the performance of read-only commands improves.
* Provides data to OLAP systems: OLAP (Online Analytical Processing) systems can use star schemas to build OLAP cubes.
* Since the data set is small and structured, the star schema is sufficient to model using ERD models and can easily be queried using SQL joins

The star schema optimized for queries on song play analysis includes the tables below:

### Fact Table
The main fact table which contains all the measures associated with each event (user song plays) is shown below:

#### Songplays Table

| COLUMN      | TYPE      | 
|---    |---    |   
|   songplay_id    | INTEGER      |
|   start_time    |  TIMESTAMP    | 
|   user_id    |   INTEGER    | 
|   level    |   VARCHAR  |   
|   song_id    |   VARCHAR   | 
|   artist_id    |   VARCHAR    | 
|   session_id    |   INTEGER    |
|   location    |   VARCHAR    |  
|   user_agent    |   VARCHAR    | 

The songplay_id field has the primary key constraint.

### Dimension Tables

The dimension tables below contain detailed information about each row in the fact table.

#### User Table

| COLUMN      | TYPE      | 
|---    |---    |   
|   user_id    | INTEGER      |  
|   first_name    |    VARCHAR    |    
|   last_name    |    VARCHAR    |   
|   gender    |    VARCHAR | 
|   level    |    VARCHAR    | 

#### Songs Table

| COLUMN      | TYPE      | 
|---    |---    | 
|   song_id    |     VARCHAR     |  
|   title    |    VARCHAR    |   
|   artist_id    |    VARCHAR    |  
|   year    |   NUMERIC | 
|   duration    |   NUMERIC    |   

#### Artists Table 

| COLUMN      | TYPE      | 
|---    |---    |  
|   artist_id    |    VARCHAR     |   
|   name    |   VARCHAR    |   
|   location    |   VARCHAR    |   
|   latitude    |   NUMERIC    |   
|   longitude    |   NUMERC   | 

#### Time Table 

| COLUMN      | TYPE      | 
|---    |---    |  
|   start_time    |  TIMESTAMP      |    
|   hour    |  INTEGER    | 
|   day    |   INTEGER    | 
|   week    |   INTEGER   | 
|   month    |   INTEGER    | 
|   year    |   INTEGER    | 
|   weekday    |   INTEGER    | 


## Project Steps

### Project Files

Below are the files related to the completion of the project:

1. data - S3 directory (s3a://udacity-dend") contains the data files, including log and song datasets.
2. dwh.cfg - configuration file containing AWS credentials
3. etl.py - reads data from S3, processes that data using Spark, and writes them back to S3
4. README.md - provides discussion and summary on this project.

### Running the project

1. The following configuration information were provided and saved in the file *dwh.cfg*.

```
[AWS]
KEY=
SECRET=
```
2. The Udacity Virtual workspace was used.

3. The  **etl.py** script was run to extract data from the files in S3, processes that data using Spark, and writes them back to my S3 ("s3a://dlake-project/").

`$ python etl.py`

The following  screenshot shows haw the files tables were arranged in my S3 bucket:

![rs_logo](https://github.com/eaamankwah/Data-Lake-with-Spark/blob/main/screenshots/mys3.png)

**Note** all the resources were deleted after the analytic testing.

### Build ETL Pipeline

1.    Load AWS credentials
2.    Implement the logic in etl.py by reading the song and log data from S3

* Song data: s3://udacity-dend/song_data
* Log data: s3://udacity-dend/log_data

3.    Data Analytic with Spark
       Implement the logic in etl.py by transforming song and log data to create five tables included in dimension Tables and Fact Table. Every table was formatted to the correct columns and data type while duplicates were removed.
4.    Load tables back to S3
Tables were partitioned and overwritten, where appropriate into parquet files in table directories on S3. Separate analytics directory was created for each of the parquet files on S3. The songs table and the time table files were partitioned by year and artist, and year and month, respectively. The Songplays table files were partitioned by year and month
5. Delete all resources when finished.

### Document Process

Do the following steps in your README.md file.
1. Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
2. State and justify your database schema design and ETL pipeline.


## References

* [Amazon EMR Console](https://us-west-2.console.aws.amazon.com/elasticmapreduce/home?region=us-west-2#.)
* [Data Lake on AWS](https://aws.amazon.com/solutions/implementations/data-lake-solution/.)
* [Udacity Q & A Platform](https://knowledge.udacity.com/?nanodegree=nd027&page=1&project=575&rubric=2502)
