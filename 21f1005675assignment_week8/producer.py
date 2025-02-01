from kafka import KafkaProducer
import pandas as pd
import time
import json

# Set Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Read data from the downloaded CSV file
df = pd.read_csv('input_data.csv')

# Send data in batches of 10
batch_size = 10
for i in range(0, len(df), batch_size):
    batch = df.iloc[i:i + batch_size].to_dict(orient='records')
    producer.send('my_topic', value=batch)
    print(f'Sent batch {i // batch_size + 1}')
    time.sleep(10)  # Sleep for 10 seconds before sending the next batch

producer.flush()
producer.close()