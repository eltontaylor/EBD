from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StringType, TimestampType, StructType, DoubleType, IntegerType

from config import configuration
def main():
    # config from mvn repo : sql-kafka, hadoop-aws, aws-java-sdk : groupId:ArtifactId:Version

    spark = SparkSession.builder.appName("sparkStreaming")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0," "org.apache.hadoop:hadoop-aws:3.3.1," "com.amazonaws:aws-java-sdk:1.11.469")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY'))\
    .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))\
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
    .getOrCreate()

    #Adjust log level to minimum
    spark.sparkContext.setLogLevel("WARN")

    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True)
    ])

    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True)
    ])

    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True),
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '2 minutes')
                )

    def streamWriter(input: DataFrame, checkpointFolder, output):
        return(input.writeStream
               .format('parquet')
               .option('checkpointLocation', checkpointFolder)
               .option('path', output)
               .outputMode('append')
               .start())

    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')

    vehicleQuery = streamWriter(vehicleDF, checkpointFolder='s3a://ebd-spark-stream-data/checkpoints/vehicle_data', output='s3a://ebd-spark-stream-data/data/vehicle_data')
    gpsQuery = streamWriter(gpsDF, checkpointFolder='s3a://ebd-spark-stream-data/checkpoints/gps_data', output='s3a://ebd-spark-stream-data/data/gps_data')
    trafficQuery = streamWriter(trafficDF, checkpointFolder='s3a://ebd-spark-stream-data/checkpoints/traffic_data', output='s3a://ebd-spark-stream-data/data/traffic_data')

    #write to s3 via multiple streams in parallel - must be last streamwriter
    trafficQuery.awaitTermination()

if __name__ == "__main__":
    main()