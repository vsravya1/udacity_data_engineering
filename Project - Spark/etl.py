import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    print('In create spark session function.....')
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    # Changing the logging level to see only Error logs on Console
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loads the song_data from AWS S3 (input_data) and extracts the songs and artist tables
    and then loaded the processed data back to S3 (output_data)
    
    :param spark: Spark Session object
    :param input_data: Location (AWS S3 path) of songs metadata (song_data) JSON files
    :param output_data: Location (AWS S3 path) where dimensional tables will be stored in parquet format 
    """  
    
    # Description: This function processes song data, reads datafrom S3, process it and loads it to S3.  
    # get filepath to song data file              
    
    #song_data = input_data + 'song_data/*/*/*/*.json'
    song_data = input_data + 'song_data/*/*/*/*.json' 
    # read song data file
    print("Reading song_data files from S3")
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data_table")
    print("song data read completed")

    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT sdt.song_id, sdt.title,sdt.artist_id,sdt.year,sdt.duration
                            FROM song_data_table sdt
                            WHERE song_id IS NOT NULL
                        """)
    
    # write songs table to parquet files partitioned by year and artist   
    print("Writing Songs table to S3 after processing")
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')
    print("songs_table load completed")

    # extract columns to create artists table
    artists_table =  spark.sql("""
                                SELECT DISTINCT art.artist_id, art.artist_name,art.artist_location,art.artist_latitude,art.artist_longitude
                                FROM song_data_table art
                                WHERE art.artist_id IS NOT NULL
                            """)
    
    # write artists table to parquet files
    print("Writing Artists table to S3 after processing")
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')
    print("artists_table load completed")


def process_log_data(spark, input_data, output_data):
    """
    Loads the log_data from AWS S3 (input_data) and extracts the songs and artist tables
    and then loaded the processed data back to S3 (output_data)
    
    :param spark: Spark Session object
    :param input_data: Location (AWS S3 path) of songs metadata (song_data) JSON files
    :param output_data: Location (AWS S3 path) where dimensional tables will be stored in parquet format             
    """
    
    #log_data = input_data + 'log_data/*.json'
    #log_data = input_data + 'log_data/2018/11/2018-11-01-events.json'
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    print("Reading log_data JSON files from S3")
    df = spark.read.json(log_data)
    print("Reading log_data JSON files from S3 completed")
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    df.createOrReplaceTempView("log_data_table")

    # extract columns for users table    
    users_table = spark.sql("""
                            SELECT DISTINCT userT.userId as user_id, 
                            userT.firstName as first_name,
                            userT.lastName as last_name,
                            userT.gender as gender,
                            userT.level as level
                            FROM log_data_table userT
                            WHERE userT.userId IS NOT NULL
                        """) 
    
    # write users table to parquet files
    print("Writing Users table to S3 after processing")  
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')
    print("users_table load completed")

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df = 
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 
    
    # extract columns to create time table
    time_table = spark.sql("""
                            SELECT 
                            A.start_time_sub as start_time,
                            hour(A.start_time_sub) as hour,
                            dayofmonth(A.start_time_sub) as day,
                            weekofyear(A.start_time_sub) as week,
                            month(A.start_time_sub) as month,
                            year(A.start_time_sub) as year,
                            dayofweek(A.start_time_sub) as weekday
                            FROM
                            (SELECT to_timestamp(timeSt.ts/1000) as start_time_sub
                            FROM log_data_table timeSt
                            WHERE timeSt.ts IS NOT NULL
                            ) A
                        """)
    
    # write time table to parquet files partitioned by year and month
    print("Writing Time table to S3 after processing")  
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_table/')
    print("time_table load completed")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs_table/')

    # extract columns from joined song and log datasets to create songplays table   
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                to_timestamp(logT.ts/1000) as start_time,
                                month(to_timestamp(logT.ts/1000)) as month,
                                year(to_timestamp(logT.ts/1000)) as year,
                                logT.userId as user_id,
                                logT.level as level,
                                songT.song_id as song_id,
                                songT.artist_id as artist_id,
                                logT.sessionId as session_id,
                                logT.location as location,
                                logT.userAgent as user_agent
                                FROM log_data_table logT
                                JOIN song_data_table songT on logT.artist = songT.artist_name and logT.song = songT.title 
                                and logT.length = songT.duration
                            """)

    # write songplays table to parquet files partitioned by year and month 1234556    
    print("Writing Songplays table to S3 after processing") 
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')
    print(" Songplays table load completed.") 


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://output-spark-proj5/"
    
    print("song_data files processing is in progress......\n")  
    process_song_data(spark, input_data, output_data)  
    print("song_data files processing completed........\n")
    
    print("log_data files processing is in progress......\n")
    process_log_data(spark, input_data, output_data)
    print("log_data files processing completed........\n")


if __name__ == "__main__":
    main()
