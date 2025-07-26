from google.cloud import storage

def fetch_gcs_content_as_bytes(gcs_uri):
    """Fetches the content of a GCS blob as bytes.

    Args:
        gcs_uri (str): The gs:// URI of the file in Google Cloud Storage.

    Returns:
        bytes: The content of the file as bytes, or None if an error occurs.
    """
    try:
        # Create a client
        storage_client = storage.Client()

        # Parse the GCS URI
        # The URI format is gs://bucket-name/object-name
        parts = gcs_uri.replace("gs://", "").split("/", 1)
        if len(parts) < 2:
            print(f"Error: Invalid GCS URI format: {gcs_uri}")
            return None

        bucket_name = parts[0]
        blob_name = parts[1]

        print(f"Attempting to fetch content from bucket: {bucket_name}, object: {blob_name}")

        # Get the bucket
        bucket = storage_client.bucket(bucket_name)

        # Get the blob (file)
        blob = bucket.blob(blob_name)

        # Download the content directly as bytes
        content_bytes = blob.download_as_bytes()

        print(f"Content of {blob_name} fetched successfully as bytes.")
        return content_bytes

    except Exception as e:
        print(f"An error occurred: {e}")
        print("Please ensure you have the correct permissions and the GCS URI is correct.")
        print("If running locally, ensure 'GOOGLE_APPLICATION_CREDENTIALS' environment variable is set or ADC is configured.")
        return None