import string
from collections import defaultdict
# from socket import *
import json
# from struct import pack,unpack
# from nltk.corpus import stopwords
# import logging
from google.cloud import storage
# import time

stopwords = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 
'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 
'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 
'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 
'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 
'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 
'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 
'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 
're', 've', 'y', 'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', 
"haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', 
"wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't"]

def mapper(p_id, mapPath, mapper_bucket, intermediate_bucket):
      
  
    print('Started mapper {}'.format(str(p_id)))
    storage_client = storage.Client()
    mapped_kv_extended = []
    # Mapper bucket
    mapper_bucket = storage.Bucket(storage_client, mapper_bucket)
    # Intermediate bucket
    intermediate_bucket = storage.Bucket(storage_client, intermediate_bucket)
    for fileName, path in mapPath:
        # print("fileName, path: ",fileName, path)
        # logging.info('Mapper {} requesting chunk data from KV Store...'.format(str(p_id)))
        
        # request_json = {
        #     'get':'mapper',
        #     'index':p_id,
        #     'path':path
        # } 
                        
        # logging.info('Mapper {} received data and now starting mapper operation...'.format(str(p_id)))
        data = ""
        blob = mapper_bucket.blob(path)
        with blob.open("r") as f:
            data = f.read()

        # Create tuples and make partitions
        remove_punct = [word for word in data.split() if word not in string.punctuation]
        data = data.translate(str.maketrans('', '', string.punctuation+"\u201c"))

        mapped_kv = [(word.lower().strip(),fileName) for word in data.split() if word.lower().strip() not in stopwords]
        mapped_kv_extended.extend(mapped_kv)
        
        
    intermediate_loc = 'intermediate_'+str(p_id)
    

    # logging.info('Mapper {} finished mapping and now starting partition operation...'.format(str(p_id)))
    # set_json = {
    #     'set' : partition_loc,
    #     'data' : partition(mapped_kv_extended),
    #     'source' : 'mapper'
    # }

    # Store data in intermediate files
    output_data = partition(mapped_kv_extended)
    
    output_blob = intermediate_bucket.blob(intermediate_loc).open("w")
    output_blob.write(json.dumps(output_data))
    output_blob.close()

    # Send request to Kv Store
    # set_json_str = json.dumps(set_json)
    # len_msg_set = pack('>Q', len(set_json_str))
    # clientSocket.send(len_msg_set)
    # clientSocket.send(set_json_str.encode())
    
    # logging.info('Mapper {} finished partition operation and data stored successfully!'.format(str(p_id)))
    print('Finished mapper {}'.format(str(p_id)))

    # clientSocket.close()  
    # logging.info('Exiting Mapper {}....'.format(str(p_id)))
    
    


def partition(mapped_list):
    mapped_dict = defaultdict(list)
    for key, value in mapped_list:
        mapped_dict[key].append(value)
    
    return mapped_dict

