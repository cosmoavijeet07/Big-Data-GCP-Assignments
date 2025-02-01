from kafka import KafkaProducer
from google.cloud import storage
import time

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='34.66.209.84:9092',
    acks='all',  # Wait for acknowledgment from all replicas
    batch_size=1  # Send each record as a separate batch
)

# Function to list and send files from Google Cloud Storage
def list_files(bucket_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix="images/")  # Adjust prefix if needed
    for blob in blobs:
        # Send each image name as a message to the Kafka topic
        producer.send('image-topic', value=blob.name.encode())
        print(f"Image sent to topic: {blob.name}")
        time.sleep(7)  # Pause between sending images

# Specify GCS Bucket Name
list_files('image-classification-project')
print("Done!")
