import multiprocessing
from socket import *
from collections import defaultdict
import sys
# from mapper_wc import mapper_wc
# from mapper_inv import mapper_inv
# from reducer_wc import reducer_wc
# from reducer_inv import reducer_inv
from multiprocessing import Process
import os
from configparser import ConfigParser
import math
import json
# from struct import pack
import logging
# import shutil
from google.cloud import storage#, datastore
from mapper import mapper
from reducer import reducer
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service-account-key.json'


class Master(object):
    def __init__(self):

        # parser = ConfigParser()
        # parser.read("./properties.config")
        # self.master_ip = parser.get('master','server_IP_address')
        # self.master_port = int(parser.get('master','server_port'))
        # self.kvStorePath = parser.get('master','kvStorePath')
        # self.outputPath = parser.get('master', 'outputPath')
        # self.logfilePath = parser.get('master','logfilePath')
        # self.n_mappers = int(parser.get('mapper','n_mapper'))
        # self.mapper_server_ip = parser.get('mapper','server_ip')
        # self.n_reducers = int(parser.get('reducer','n_reducer'))
        # self.reducer_server_ip = parser.get('reducer','server_ip')
        # self.partitionPath = parser.get('master','partitionPath')
        # self.mapPath = parser.get('master','mapPath')
        # self.app = parser.get('app','operation')
        # self.wcInputPath = parser.get('app','wc_input_path')
        # self.invInputPath = parser.get('app','inv_input_path')
        # Create a bucket on google cloud storage
        self.bucket_name = "input_store"
        self.mapper_bucket = "partitions_anurag"
        self.partition_bucket = "intermediate_anurag"
        self.output_bucket = "output_anurag"


    def init_cluster(self, ip_port):
        
        if len(ip_port) > 1 or len(ip_port) == 0:
            print('System takes a single IP address and port currently, please retry')
            sys.exit()
        else:
            try:
                self.host = ip_port[0][0]
                self.serverPort = int(ip_port[0][1])
            except:
                print('Invalid arguments!')
                sys.exit()
            self.clientSocket = socket(AF_INET, SOCK_STREAM)
            self.clientSocket.connect((self.host, self.serverPort))
            self.mapper_server_ip = ip_port[0][0] + ':' + str(ip_port[0][1])
            self.reducer_server_ip = ip_port[0][0] + ':' + str(ip_port[0][1])
            logging.basicConfig(filename = self.logfilePath, format="%(asctime)s %(levelname)s: %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p", level=logging.INFO)
        return self.clientSocket
        
        

    def run_mapred(self, input_data, map_fn, reduce_fn, output_location):
        list_invMap = []
        try:
            multiprocessing.set_start_method('fork', force = True)
        except: 
            pass

        self.mapPath = output_location+'/map/'
        if not os.path.exists(self.mapPath):
            os.mkdir(output_location+'/map/')

        # if not os.path.exists(self.outputPath):
            # os.mkdir(self.outputPath)

        self.partitionPath = output_location+'/partition'
        if not os.path.exists(self.partitionPath):
            os.mkdir(output_location+'/partition')

        # MapReduce for Word count
        if map_fn == 'mapper_wc' and reduce_fn == 'reducer_wc':
            print('Getting word count for input file....')
            
            self.wcMapPath = self.mapPath+'/wc/'
            if not os.path.exists(self.wcMapPath):
                os.mkdir(self.wcMapPath)
            
            self.wcPartitionPath = self.partitionPath + '/wc/'
            if not os.path.exists(self.wcPartitionPath):
                os.mkdir(self.wcPartitionPath)
            
            self.wcOutputPath = output_location+ '/output_wc/'
            if not os.path.exists(self.wcOutputPath):
                os.mkdir(self.wcOutputPath)
            
            # Split the file into multiple files as per the number of mappers
            logging.info('Splitting files...')
            map_paths = self.split_files_v2(input_data, self.n_mappers, self.clientSocket, self.wcMapPath, flag='wc')
            
            logging.info('Starting mapper processes...')
            # Run mapper function
            self.run_mapper(self.n_mappers, self.mapper_server_ip, self.wcPartitionPath, map_paths, op_flag='wc')
            logging.info('Finished mapper processes!')

            logging.info('Starting reducer processes...')
            # Run reducer function
            self.run_reducer(self.n_reducers, self.reducer_server_ip, self.wcOutputPath, self.n_mappers, self.wcPartitionPath, op_flag = 'wc')
            logging.info('Finished reducer processes!')
            logging.info('Finished Map Reduce operations, Goodbye!')

        # MapReduce for Inverted Index
        elif map_fn == 'mapper_inv' and reduce_fn == 'reducer_inv':
            print('Getting inverted indexes for input files...')
            
            self.invMapPath = self.mapPath+'/inv/'
            if not os.path.exists(self.invMapPath):
                os.mkdir(self.invMapPath)

            self.invPartitionPath = self.partitionPath + '/inv/'
            if not os.path.exists(self.invPartitionPath):
                os.mkdir(self.invPartitionPath)

            self.invOutputPath = output_location + '/output_inv/'
            if not os.path.exists(self.invOutputPath):
                os.mkdir(self.invOutputPath)

            # Split the file into multiple files as per the number of mappers
            logging.info('Splitting files...')
            for i, filename in enumerate(os.listdir(self.invInputPath)):
                inputPath = os.path.join(self.invInputPath, filename)
                map_paths = self.split_files_v2(inputPath, self.n_mappers, self.clientSocket, self.invMapPath, flag='inv')
                list_invMap.extend(map_paths)
                map_paths = list_invMap
            
            logging.info('Starting mapper processes...')
            # Run mapper function
            self.run_mapper(self.n_mappers, self.mapper_server_ip, self.invPartitionPath, map_paths, op_flag='inv')
            logging.info('Finished mapper processes!')

            logging.info('Starting reducer processes...')
            # Run reducer function
            self.run_reducer(self.n_reducers, self.reducer_server_ip, self.invOutputPath, self.n_mappers, self.invPartitionPath, op_flag = 'inv')
            logging.info('Finished reducer processes!')
            logging.info('Finished Map Reduce operations, Goodbye!')

        else:
            print('Please input valid function (mapper_wc, reducer_wc/mapper_inv, reducer_inv)')
            logging.info('Please input valid function (mapper_wc, reducer_wc/mapper_inv, reducer_inv') 
            sys.exit()
        


        
    
    def destroy_cluster(self, clientSocket, flag = 0):
        if flag == 1:
            try:
                shutil.rmtree(self.mapPath)
            except:
                pass
            try:
                shutil.rmtree(self.partitionPath)
            except:
                pass
        clientSocket.close()
        logging.info('Cleaned up .')

    def delete_files(self):
        storage_client = storage.Client()
        mapper_bucket = storage.Bucket(storage_client, self.mapper_bucket)
        partition_bucket = storage.Bucket(storage_client, self.partition_bucket)

        for blob in mapper_bucket.list_blobs():
            blob.delete()
        
        for blob in partition_bucket.list_blobs():
            blob.delete()
    

    def split_files(self, input_file_path, n_mappers, flag = None):
        map_paths = []
        
        print("Inside split files function")
        # Get input bucket
        storage_client = storage.Client()
        bucket = storage.Bucket(storage_client, self.bucket_name)

        # Create bucket if not exists
        output_bucket = storage.Bucket(storage_client, self.mapper_bucket)
        
        if not output_bucket.exists():
            output_bucket = storage_client.create_bucket(self.mapper_bucket, location = 'US')

        # Get all blobs in input bucket
        blobs = bucket.list_blobs()
        for blob in blobs:
            # print(blob.name)

        # return [blob.name for blob in blobs]
            fileName = blob.name


            # print("Found bucket!")
            blob_input = bucket.blob(fileName)
            
            file_size = bucket.get_blob(fileName).size
            print("Blob size: ",file_size)
            chunkSize = math.ceil(file_size/n_mappers)
            print("chunkSize: ",chunkSize)
            chunk_no = 0
            print('Splitting files')
            
            # input_file_path = 'gs://input_store/doc1.txt'
            # map_path = 'gs://input_store/'
            # fileName = input_file_path.split('/')[-1]
            
            with blob_input.open("r") as f:
                while True:
                    data = f.read(chunkSize)
                    if not data:
                        break 
                    # if flag == 'wc':
                    #     path = mapPath+'mapper_'+str(chunk_no)+'.txt'
                    # elif flag == 'inv':
                    #     path = mapPath+'mapper_'+fileName+'_'+str(chunk_no)+'.txt'
                    path = 'mapper_'+fileName.split(".")[0]+'_'+str(chunk_no)+'.txt'
            

                    map_paths.append(path)
                    
                    
                    # Create a kv store file in the bucket on GCS
                    # print(bucket)
                    blob_output = output_bucket.blob(path).open("w")
                    blob_output.write(data)
                    blob_output.close()

                    print(len(data))
                    print("Chunk number: ",chunk_no)
                    # Write to file
                    chunk_no += 1
                    # with blob_output.open("w") as f:
                    #     f.write(data)


                # del data
        
        return map_paths

    

    def run_mapper(self, n_mappers, map_paths):

        # Temp fork call
        try:
            multiprocessing.set_start_method('fork', force = True)
        except: 
            pass

        mappers = []
        partitions_loc = []
        
        # if op_flag == 'inv':
        map_dict = defaultdict(list)
        for path in map_paths:
            path_spl = path.split('/')[-1].split('_')
            map_file = path_spl[1]
            map_ind = int(path_spl[-1].split('.')[0])
            map_dict[map_ind].append((map_file, path))
        map_paths = map_dict
        mapper_fn = mapper
        print(map_dict)
        # return None
        # elif op_flag == 'wc':
        #     mapper_fn = mapper_wc
            

        for p_id in range(n_mappers):
            # q = multiprocessing.Queue()
            
            proc = Process(target=mapper_fn, args=(p_id,map_paths[p_id],self.mapper_bucket,self.partition_bucket,))#,q,IP_list[p_id]))
            proc.start()
            logging.info('Started mapper process: {}'.format(str(p_id)))
            mappers.append(proc)
            #partitions_loc.append(q.get())
        
        # Barrier handling
        [p.join() for p in mappers]
        return partitions_loc

    
    def run_reducer(self, n_mappers, n_reducers, op_flag = None):
        reducers = []

        # if op_flag == 'inv':
        #     reducer_fn = reducer_inv
        # else:
        #     reducer_fn = reducer_wc


        for p_id in range(n_reducers):
            proc = Process(target=reducer, args=(p_id, n_mappers, n_reducers, self.partition_bucket, self.output_bucket))
            proc.start()
            logging.info('Started reducer process: {}'.format(str(p_id)))
            
            reducers.append(proc)
        
        # Barrier handling
        [p.join() for p in reducers]
        
 

if __name__ == "__main__":
    

    master = Master()
    
    clientSocket = master.init_cluster([(master.master_ip, master.master_port)])
    # print(master.mapper_server_ip,master.reducer_server_ip)
    # master.destroy_cluster(clientSocket)
    # sys.exit()
   
    # if not os.path.exists(master.mapPath):
    #     os.mkdir(master.mapPath)
   
    # if not os.path.exists(master.outputPath):
    #     os.mkdir(master.outputPath)
   
    # if not os.path.exists(master.partitionPath):
    #     os.mkdir(master.partitionPath)
   
    # master.wcMapPath = master.mapPath+'/wc/'
    # if not os.path.exists(master.wcMapPath):
    #     os.mkdir(master.wcMapPath)
   
    # master.invMapPath = master.mapPath+'/inv/'
    # if not os.path.exists(master.invMapPath):
    #     os.mkdir(master.invMapPath)
   
    # master.wcPartitionPath = master.partitionPath + '/wc/'
    # if not os.path.exists(master.wcPartitionPath):
    #     os.mkdir(master.wcPartitionPath)
   
    # master.invPartitionPath = master.partitionPath + '/inv/'
    # if not os.path.exists(master.invPartitionPath):
    #     os.mkdir(master.invPartitionPath)
   
    master.wcOutputPath = master.outputPath + '/wc/'
    if not os.path.exists(master.wcOutputPath):
        os.mkdir(master.wcOutputPath)
   
    # master.invOutputPath = master.outputPath + '/inv/'
    # if not os.path.exists(master.invOutputPath):
    #     os.mkdir(master.invOutputPath)

    # logging.basicConfig(filename = master.logfilePath, format="%(asctime)s %(levelname)s: %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p", level=logging.INFO)
    # logging.info('Starting map reduce operations....')

    master.run_mapred(master.wcInputPath, 'mapper_wc', 'reducer_wc', master.wcOutputPath)

    # Socket connection
    # host = master.master_ip
    # serverPort = master.master_port
    # clientSocket = socket(AF_INET, SOCK_STREAM)
    # clientSocket.connect((host, serverPort))

    # Split the file into multiple files as per the number of mappers
    # logging.info('Starting the process to split the input into chunks..')
    # list_invMap = []
    # if master.app == 'wc':
    #     map_paths = master.split_files_v2(master.wcInputPath, master.n_mappers, clientSocket, master.wcMapPath, flag='wc')
    
    # elif master.app == 'inv':
    #     for i, filename in enumerate(os.listdir(master.invInputPath)):
    #         inputPath = os.path.join(master.invInputPath, filename)
    #         map_paths = master.split_files_v2(inputPath, master.n_mappers, clientSocket, master.invMapPath, flag='inv')
    #         list_invMap.extend(map_paths)
    #         map_paths = list_invMap

    # else:
    #     print('Please input valid application in Config file (wc/inv)')
    #     logging.info('Please input valid application in Config file (wc/inv)') 
    #     sys.exit()

    # logging.info('Finished splitting the input into chunks!')
    
    # # Run the mapper function
    # logging.info('Starting mapper processes...')

    # if master.app == 'wc':
    #     master.run_mapper(master.n_mappers, master.mapper_server_ip, master.wcPartitionPath, map_paths, op_flag='wc')
    
    # elif master.app == 'inv':    
    #     master.run_mapper(master.n_mappers, master.mapper_server_ip, master.invPartitionPath, map_paths, op_flag='inv')
    
    # logging.info('Finished mapper processes!')

    # # Run the reducer function
    # logging.info('Starting reducer processes...')
    # if master.app == 'wc':
    #     master.run_reducer(master.n_reducers, master.reducer_server_ip, master.wcOutputPath, master.n_mappers, master.wcPartitionPath, op_flag = 'wc')
    
    # elif master.app == 'inv':
    #     master.run_reducer(master.n_reducers, master.reducer_server_ip, master.invOutputPath, master.n_mappers, master.invPartitionPath, op_flag = 'inv')

    
    # logging.info('Finished reducer processes!')



    master.destroy_cluster(clientSocket, flag = 1)
    logging.info('Finished Map Reduce operations, Goodbye!')
    
    # clientSocket.close()    
