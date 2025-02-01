from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, minute, when, to_timestamp


spark = SparkSession.builder.appName("UserClicksHashing").getOrCreate()

#file path

gcs_path = "gs://week4g3/sample.csv"


clicks_df = spark.read.csv(gcs_path, header=True, inferSchema=True)


clicks_df = clicks_df.withColumn("time", to_timestamp("time", "HH:mm"))

#hashing

def hash_timestamp(hour_value, minute_value):
    return when((hour_value >= 0) & (hour_value < 6), "0-6") \
           .when((hour_value >= 6) & ((hour_value < 12) | ((hour_value == 12) & (minute_value == 0))), "6-12") \
           .when((hour_value >= 12) & ((hour_value < 18) | ((hour_value == 12) & (minute_value > 0))), "12-18") \
           .otherwise("18-24")

clicks_count_df = clicks_df.select(hour(col("time")).alias("hour"), minute(col("time")).alias("minute")) \
    .groupBy(hash_timestamp(col("hour"), col("minute")).alias("time_interval")) \
    .count() \
    .orderBy("time_interval")

#display
clicks_count_df.show()

spark.stop()