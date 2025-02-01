from google.cloud import storage
import pandas as pd
from kafka import KafkaProducer
import json
import time
from io import BytesIO


producer = KafkaProducer(
    bootstrap_servers='34.28.48.166:9092',
    acks='all',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Ensure it's properly serialized
)

client = storage.Client()
bucket = client.bucket("stock-oppe")
blobs = bucket.list_blobs(prefix="NSE_Stocks_Data/")

csv_files = {}
for blob in blobs:
    if blob.name.endswith(".csv"):
        print(f"Downloading: {blob.name}")
        content = blob.download_as_bytes()
        df = pd.read_csv(BytesIO(content))
        csv_files[blob.name] = df

dataframes = {}

for file_name, df in csv_files.items():
    company_name = file_name.split("/")[-1]
    company_name = company_name.split("_")[0]
    df['company'] = company_name
    dataframes[file_name] = df

# Initialize sliding windows for each CSV file
windows = {file_name: [] for file_name in csv_files}
indices = {file_name: 0 for file_name in csv_files}  

batch_number = 0
while True:
    batch_number += 1
    print(f"Producing batch #{batch_number}...")

    for file_name, df in dataframes.items():
        current_index = indices[file_name]
        if current_index >= len(df):
            continue
        windows[file_name].append(df.iloc[current_index].to_dict())
        if len(windows[file_name]) > 10:
            windows[file_name].pop(0)

        indices[file_name] += 1
        windowed_df = pd.DataFrame(windows[file_name])
        windowed_data = windowed_df.groupby('company').agg(
            avg_volume=('volume', 'mean'),
            last_close=('close', 'max')
        ).reset_index()

        merged_df = windowed_df.merge(windowed_data, on='company', how='left')

        merged_df['A1'] = abs((merged_df['close'] - merged_df['last_close']) / merged_df['last_close']) > 0.005
        merged_df['A2'] = merged_df['volume'] > merged_df['avg_volume'] * 1.02
        merged_df['is_anomaly'] = merged_df['A1'] | merged_df['A2']

        anomalies = merged_df[merged_df['is_anomaly']]

        for index, row in anomalies.iterrows():
            record = {
                "company": row["company"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
                "avg_volume": row["avg_volume"],
                "last_close": row["last_close"],
                "A1": row["A1"],
                "A2": row["A2"],
                "is_anomaly": row["is_anomaly"]
            }
            print(f"Sending record {index}")
            producer.send("stock-oppe", record)
    print(f'Batch {batch_number} with {len(anomalies)} anomally rows send successfully')

    if all(indices[file_name] >= len(dataframes[file_name]) for file_name in csv_files):
        print("All data processed. Exiting.")
        break

    producer.flush()
    time.sleep(1)

producer.close()
