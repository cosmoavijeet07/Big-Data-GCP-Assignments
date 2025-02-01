from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

spark = SparkSession.builder \
    .appName("StockAnomalyDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

json_schema = StructType([
    StructField("company", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("avg_volume", DoubleType(), True),
    StructField("last_close", DoubleType(), True),
    StructField("A1", BooleanType(), True),
    StructField("A2", BooleanType(), True),
    StructField("is_anomaly", BooleanType(), True)
])

stock_data_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "34.28.48.166:9092") \
    .option("subscribe", "stock-oppe") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as value")

anomalies = stock_data_stream \
    .select(from_json(col("value"), json_schema).alias("data")) \
    .select("data.*") \
    .filter(col("data").isNotNull())

query = anomalies.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="1 seconds") \
    .start()

query.awaitTermination()
