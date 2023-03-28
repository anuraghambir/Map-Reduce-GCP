import os
from google.cloud import storage
import sys
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = sys.argv[2]
file_path = sys.argv[1]
file_name = file_path.split("/")[-1]
# os.system("gcloud storage cp A\ Room\ With\ A\ View.txt  gs://ip_source/")
storage_client = storage.Client()
# Intermediate bucket
bucket = storage.Bucket(storage_client, "input_store")
# for blob in bucket.list_blobs():
# 	print(blob.name)

data = ""
with open(file_path) as f:
	data = f.read()

blob_output = bucket.blob(file_name).open("w")
blob_output.write(data)
blob_output.close()
