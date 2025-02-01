from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from kafka import KafkaConsumer
import io
import json
from PIL import Image
import torch
import requests
from torchvision import models, transforms
from google.cloud import storage

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ImageClassificationStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.executorEnv.TORCH_HOME", "/tmp/torch_cache") \
    .getOrCreate()

# Kafka Source
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "34.66.209.84:9092") \
    .option("subscribe", "image-topic") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", "10") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as path")

# ImageNet Labels Loader
def load_imagenet_labels():
    url = "https://raw.githubusercontent.com/anishathalye/imagenet-simple-labels/master/imagenet-simple-labels.json"
    response = requests.get(url)
    return json.loads(response.text)

imagenet_labels = load_imagenet_labels()

# UDF for Image Classification
def classify_udf(path):
    try:
        client = storage.Client()
        bucket = client.get_bucket('image-classification-project')
        blob = bucket.get_blob(path)
        content = blob.download_as_bytes()

        transform = transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
        ])
        image = Image.open(io.BytesIO(content)).convert("RGB")
        image_tensor = transform(image)

        model = models.mobilenet_v2(pretrained=True)
        model.eval()
        with torch.no_grad():
            predictions = model(image_tensor.unsqueeze(0))
        class_index = predictions.argmax().item()
        return json.dumps({"index": class_index, "label": imagenet_labels[class_index]})
    except Exception as e:
        return json.dumps({"error": str(e)})

# Register UDF
classify_spark_udf = udf(classify_udf, StringType())

# Apply UDF and Write Stream
predictions = df.withColumn("prediction", classify_spark_udf(col("path")))

query = predictions.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='10 seconds') \
    .start()

query.awaitTermination()
