from google.cloud import storage
def download_file_from_gcs(bucket_name, source_blob_name, destination_file_name):
    """Downloads a file from GCS."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f"File {source_blob_name} downloaded to {destination_file_name}.")

def count_lines_in_file(file_path):
    """Counts the number of lines in a file."""
    with open(file_path, 'r') as file:
        line_count = sum(1 for line in file)
    return line_count

if __name__ == "__main__":
    bucket_name = 'ga_12'
    source_blob_name = 'ga_1.txt'
    destination_file_name = '/tmp/result'

    # Download the file from GCS
    download_file_from_gcs(bucket_name, source_blob_name, destination_file_name)

    # Count lines in the downloaded file
    line_count = count_lines_in_file(destination_file_name)
    print(f"The file has {line_count} lines.")
