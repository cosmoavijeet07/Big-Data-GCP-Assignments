from google.cloud import storage

def count_lines_in_file(bucket_name , file_name):
    # Initializing the client
    client = storage.Client()

    # Geting the bucket
    bucket = client.bucket(bucket_name)

    # Geting the file
    blob = bucket.blob(file_name)

    # Downloading the file as a string
    file_content = blob.download_as_string()

    # Counting the number of lines
    lines_count = file_content.count(b'\n') + 1

    return lines_count

def count_lines_gcs(event, context):

    # Extracting the bucket and file name from the event data
    bucket_name = event['bucket']
    file_name = event['name']

    # Counting the lines from the file
    lines_count = count_lines_in_file(bucket_name, file_name)

    # print the result
    print(f'Number of lines in the {file_name}: {lines_count}')
