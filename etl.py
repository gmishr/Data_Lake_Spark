import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

"""
In below code we are reading dl.cfg file
for aws credetials
"""
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

"""
Below we are setting spark session in order to use spark
"""
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

"""
Below function is written to process song data file 
which reads data from s3 and loads different parquet 
folders in s3 after processing. This function has three arguments:
spark: This is passing spark session
input_data : This is passing input s3 bucket path
output_data : This is passing output s3 bucket path
"""

def process_song_data(spark, input_data, output_data):
    """Below we are setting song file path"""
    #song_data = "{}song_data/A/B/C/TRABCEI128F424C983.json".format(input_data)
    song_data = "{}song-data/A/A/A/*.json".format(input_data)
    
    """Below we are reading file"""
    df = spark.read.json(song_data)

    """In below part code we are extracting required 
    columns from song data to load songs_table"""
    
    songs_table = df.select(["song_id","title","artist_id","year","duration"])
    songs_table = songs_table.dropDuplicates(["song_id"])
    
    songs_table.write.partitionBy("year","artist_id").parquet("{}/parquet/songs.parquet".format(output_data))
    print("Songs table data loaded in S3.")

    """In below part code we are extracting required 
    columns from song data to load artists_table"""
    
    artists_table = df.select(["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"])
    artists_table = artists_table.dropDuplicates(["artist_id"])
    
    artists_table.write.parquet("{}/parquet/artists.parquet".format(output_data))
    print("artists table data loaded in S3.")

"""
Below function is written to process log data file 
which reads data from s3 and loads different parquet 
folders in s3 after processing. This function has three arguments:
spark: This is passing spark session
input_data : This is passing input s3 bucket path
output_data : This is passing output s3 bucket path
"""


def process_log_data(spark, input_data, output_data):
    """Below we are setting log file path"""
    #log_data = "{}log_data/2018/11/2018-11-12-events.json".format(input_data)
    log_data = "{}log_data/*/*/*.json".format(input_data)
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    """In below part code we are extracting required 
    columns from log data to load users_table"""
    
    users_table = df.select(["userId","firstName","lastName","gender","level"])
    users_table.dropDuplicates(["userId"])
    
    # write users table to parquet files
    users_table.write.parquet("{}/parquet/users.parquet".format(output_data))
    print("users table data loaded in S3.")

    """In below part of code we have wriiten function 
    to get timestamp from ts column of log data"""
    
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn("timestamp", get_timestamp(df.ts)) 
    
    """In below part of code we have wriiten function 
    to get datetime from ts column of log data"""
    
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    """In below part code we are extracting required 
    columns from log data to load time_table"""
    
    time_table = df.select(
        col("datetime").alias("start_time"),
        hour("datetime").alias("hour"),
        dayofmonth("datetime").alias("day"),
        weekofyear("datetime").alias("week"),
        month("datetime").alias("month"),
        year("datetime").alias("year")
        )
    time_table = time_table.withColumn("weekday", date_format(col("start_time"), "u"))
    time_table.dropDuplicates(["start_time"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet("{}/parquet/time.parquet".format(output_data))
    print("time table data loaded in S3.")

    """Below we are setting song file path"""
    #song_data = "{}song_data/A/B/C/TRABCEI128F424C983.json".format(input_data)
    song_data = "{}song-data/A/A/A/*.json".format(input_data)
    song_df = spark.read.json(song_data)

    """Here we are joining two dataframe song_df and 
    log_df on required columns to extract all required columns to load songplays_table"""
    
    df = df.join(song_df, (song_df['title'] == df['song']) & 
                 (song_df['artist_name'] == df['artist']) & (song_df['duration'] == df['length']),"LeftOuter")
    
    """In below part code we are extracting required 
    columns from joined dataframe to load songplays_table"""
    
    songplays_table = df.select(
        col("datetime").alias("start_time"),
        col("userId").alias("user_id"),
        col("level").alias("level"),
        col("song_id").alias("song_id"),
        col("artist_id").alias("artist_id"),
        col("sessionId").alias("session_id"),
        col("location").alias("location"),
        col("userAgent").alias("user_agent"),
        year("datetime").alias("year"),
        month("datetime").alias("month")
        )
    
    songplays_table.createOrReplaceTempView('songplays_table')
    songplays_table = spark.sql('select row_number() over (order by "songplay_id") as songplay_id, * from songplays_table')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet("{}/parquet/songplays.parquet".format(output_data))
    print("songplays table data loaded in S3.")

"""
In below part of code we have written main function.
"""
def main():
    """
    Here we are setting s3 bucket variables from input and output path
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://temp-spark-udacity-8796/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
