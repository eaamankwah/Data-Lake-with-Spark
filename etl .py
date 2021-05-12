import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as func

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    
    
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    The function reads the song data in the filepath, reads song and artist data
    to complete the songs and artists dimensional tables.
    """
    
    # get filepath to song data file
    #song_data = 'data/song_data/A/A/A/*.json'
    song_data = input_data + 'song_data/A/A/A/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    print('Read song_data successfull from S3')

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id','year', 'duration') \
            .dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs/',mode='overwrite')
    print('Wrote songs_table to parquet successfully')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',
                                  'artist_latitude', 'artist_longitude') \
        .withColumnRenamed('artist_name', 'name') \
        .withColumnRenamed('artist_location', 'location') \
        .withColumnRenamed('artist_latitude', 'latitude') \
        .withColumnRenamed('artist_longitude', 'longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists_table/',mode='overwrite')
    print('Wrote artists_table to parquet successfully')


def process_log_data(spark, input_data, output_data):
    """
    This function reads the log data in the
    filepath to get data to complete the user, time and song
    dimensional tables and the songplays fact table.
    """
    
    # get filepath to log data file
    
    #log_data = 'data/log_data/*.json'
    log_data = input_data + 'log_data/*/*/*.json'
    
    # read log data file
    df = spark.read.json(log_data)
    print('Read log_data from S3 successfully')
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # extract columns for users table
    users_table = df.select('userId', 'firstName', 'lastName','gender', 'level') \
                                .dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users_table/',mode='overwrite')
    print('Wrote users_table to parquet successfully')

    # create timestamp column from original timestamp column
    df = df.withColumn('start_time', func.from_unixtime(func.col('ts')/1000))
    print('Converted ts to timestamp successfully')
    
    # create datetime column from original timestamp column
        
    # extract columns to create time table
    time_table = df.select('ts', 'start_time') \
        .withColumn('hour', func.hour('start_time')) \
        .withColumn('day', func.dayofyear('start_time')) \
        .withColumn('week', func.weekofyear('start_time')) \
        .withColumn('month', func.month('start_time')) \
        .withColumn('year', func.year('start_time')) \
        .withColumn('weekday', func.dayofweek('start_time')).dropDuplicates()
    print('Extracted DateTime Columns sucessfully')

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time_table/',mode='overwrite')
    print('Wrote time_table to parquet sucessfully')
                                
    # read in song data to use for songplays table
    song_data = input_data + 'song_data/A/A/A/*.json'
    song_dataset = spark.read.json(song_data)
    print('Read song_dataset from S3 sucessfully')

    # extract columns from joined song and log datasets to create songplays table
    song_dataset.createOrReplaceTempView('song_dataset')
    time_table.createOrReplaceTempView('time_table')
    df.createOrReplaceTempView('log_dataset')

    songplays_table = spark.sql("""SELECT DISTINCT
                                        ld.ts as ts,
                                        tt.year as year,
                                        tt.month as month,
                                        ld.userId as user_id,
                                        ld.level as level,
                                        sd.song_id as song_id,
                                        sd.artist_id as artist_id,
                                        ld.sessionId as session_id,
                                        sd.artist_location as artist_location,
                                        ld.userAgent as user_agent
                                    FROM song_dataset sd
                                    JOIN log_dataset ld
                                        ON sd.artist_name = ld.artist
                                        AND sd.title = ld.song
                                        AND sd.duration = ld.length
                                    JOIN time_table tt
                                        ON tt.ts = ld.ts
                                    """).dropDuplicates()
    print('Query run successfully')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'songplays/', mode='overwrite')
    print('Wrote songplays_table to parquet successfully')

def main():
    spark = create_spark_session()
    #input_data='data/'
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dlake-project/" 
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
