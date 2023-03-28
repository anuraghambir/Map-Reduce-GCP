import master
from google.cloud import storage
from collections import defaultdict
import json

def master_main(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()
    if request.args and 'message' in request.args:
        return request.args.get('message')
    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        # return f'Hello World!'
        print("Inside main function")
        m = master.Master()        
        print("Created master object")
        # map_paths = m.split_files('A Room With A View.txt', 3, flag = None)
        m = master.Master()        
        n_mappers = 3
        n_reducers = 3
        map_paths = m.split_files('A Room With A View.txt', n_mappers, flag = None)
        m.run_mapper(n_mappers, map_paths)
        m.run_reducer(n_mappers, n_reducers)

        storage_client = storage.Client()
        output_bucket = storage.Bucket(storage_client, "output_anurag")


        final_dict = defaultdict()
        blobs = output_bucket.list_blobs()
        for blob in blobs:
            # fileName = blob.name
            partition_kv = defaultdict()
            with blob.open("r") as f:
                partition_kv = json.load(f)
            final_dict.update(partition_kv)
            blob.delete()

        output_blob = output_bucket.blob("output.txt").open("w")
        output_blob.write(json.dumps(final_dict))
        output_blob.close()

        m.delete_files()
        return f"Finished processing"