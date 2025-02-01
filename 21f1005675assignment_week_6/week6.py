from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import to_date
import datetime
import random

# Initialize SparkSession
sc = SparkSession.builder.appName("IBD_week6").getOrCreate()

# Set legacy time parser policy to handle 'dd-MM-yyyy' format
sc.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Define schema
schema = StructType([
    StructField("Sno", IntegerType(), False),
    StructField("PID", IntegerType(), False),
    StructField("Name", StringType(), False),
    StructField("Description", StringType(), False),
    StructField("Start", StringType(), False),
    StructField("End", StringType(), True)
])

# Load old data from Cloud Storage
old = sc.read.csv("gs://week6-buc/input.csv", header=True, schema=schema)

# Convert 'Start' and 'End' columns to DateType
old = old.withColumn("Start", to_date("Start", "dd-MM-yyyy")) \
         .withColumn("End", to_date("End", "dd-MM-yyyy"))

# Display current table
print("\nCurrent Table")
old.show()

# Create a temporary view for old data
old.createOrReplaceTempView("old")

# Function to generate random dates
def random_date(start_year, end_year):
    start_date = datetime.date(start_year, 1, 1)
    end_date = datetime.date(end_year, 12, 31)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + datetime.timedelta(days=random_days)

# Create new data with person names and updated descriptions
new = sc.createDataFrame([
    (5, 11, "Aman", "Diploma", random_date(2023, 2024), None),
    (6, 12, "Rohit", "Degree", random_date(2023, 2024), None),
    (7, 13, "Sneha", "Foundation", random_date(2023, 2024), None)
], schema=StructType([
    StructField("Sno", IntegerType(), False),
    StructField("PID", IntegerType(), False),
    StructField("Name", StringType(), False),
    StructField("Description", StringType(), False),
    StructField("Start", DateType(), False),
    StructField("End", DateType(), True)
]))

# Create a temporary view for new data
new.createOrReplaceTempView("new")

# Update records by joining old and new data
update = sc.sql("""
    SELECT 
        old.Sno, old.PID, old.Name, old.Description, old.Start, 
        CASE WHEN new.Sno IS NULL THEN old.End ELSE new.Start END AS End 
    FROM old 
    LEFT JOIN new ON old.PID = new.PID
""")

# Create a temporary view for updated data
update.createOrReplaceTempView("updated")

# Display updated records table
print("\nUpdated Records Table")
update.show()

# Combine updated and new records
result = sc.sql("SELECT * FROM updated UNION SELECT * FROM new")

# Display final dataframe
print("\nFinal DataFrame")
result.show()

# Save result to Cloud Storage
result.write.csv("gs://week6-buc/output.csv", header=True)
