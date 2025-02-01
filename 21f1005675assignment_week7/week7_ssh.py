from google.cloud import pubsub_v1, storage
import time

def callback(message):
    """Callback function to handle incoming messages."""
    filename = message.data.decode('utf-8')
    print(f"Received message for file: {filename}")
    try:
        lines = lines_counter(filename)
        print(f"The number of lines in {filename} are {lines}")
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
    finally:
        # Acknowledge the message to remove it from the subscription
        message.ack()

def lines_counter(filename):
    """Count the number of lines in a file stored in Google Cloud Storage."""
    client = storage.Client()
    bucket = client.get_bucket('week7-buc')
    blob = bucket.blob(filename)
    
    if not blob.exists():
        raise FileNotFoundError(f"The file {filename} does not exist in the bucket.")
    
    with blob.open('r') as file:
        lines = sum(1 for line in file)
    return lines

def subscribe_pub_sub(project_id, subscription_name):
    """Subscribe to a Pub/Sub subscription and listen for messages."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    print("Listening for messages on subs...")
    try:
        # Subscribe and keep the event loop active
        future = subscriber.subscribe(subscription_path, callback=callback)
        future.result()
        while True:
            time.sleep(1)  # Prevent high CPU usage
    except KeyboardInterrupt:
        print("Shutting down subscriber...")
        future.cancel()  # Cancel the subscriber's listening task
        subscriber.close()  # Gracefully close the subscriber

if __name__ == "__main__":
    project_id = 'week-3-idb'
    subscription_name = 'subs'
    subscribe_pub_sub(project_id, subscription_name)
