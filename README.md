## Aim:
Aim of this project is to design and implement a parallel map reduce system using Google Cloud functions.
Github link for the project: https://github.com/anuraghambir/Map-Reduce-GCP


## Design Details:
1. Cloud functions are used to create the master, mapper, and reducer. For
this function, HTTP trigger is used.
2. All the data, including input, intermediate, and output files are all stored in
their respective buckets on Google Cloud Storage.
3. Initially, only the input files are stored in the input bucket.
4. Mappers and Reducers run in parallel by using the multiprocessing method.
5. Barrier Synchronization is implemented to ensure all the parallel mappers
and reducers finish before moving on to the next part.
6. A final output file is created in the output bucket which contains the
aggregated data from each of the reducers.
7. Inverted Index functionality is implemented in the Map-Reduce process
with the final output of each term being stored in descending order of the
frequency of occurrence of each term.
8. After the process is finished, all the intermediate files are deleted with only
the output file being stored in the output bucket.

## Front End:
1. A front-end is created using HTML and Javascript is used to search for the
occurrence of a term in the input files.
2. You can access the front-end by clicking on the following link,
https://storage.googleapis.com/web-ui-anurag/web-ui.html
3. You can then enter a term you want, for example: ‘project’ and you will get
the output as following
4. This is done by creating a cloud function in the backend with a HTTP trigger
that points to the output file. If we want to search for a term, this cloud
function is triggered which then searches for this term in the file and returns
the value for the respective term.



## Streaming Search:
1. Whenever a new file is added in the output bucket, the map reduce
operation is invoked through a trigger and a new output file is generated.
2. This is done by using ‘Storage Trigger’ based cloud function. Internally,
this function invokes the HTTP Triggered Map-Reduce cloud function
which performs the inverted index operation and stores the output.
3. Changes can be seen in the Web-UI.
4. The user can also additionally add another file in the input_store bucket.
This can be done by running a python file and passing the address of the
text file which needs to be uploaded as well as the service account key
through command line arguments.

## Execution instructions:
1. User can click on the url: https://storage.googleapis.com/web-uianurag/
web-ui.html
2. After clicking the url, you can enter the term in search box and a list of files
containing the term in descending order of its frequency in the files will be
obtained.
3. A new file can be added to the input bucket through the following command.
python3 src/add_file_to_gcs.py <file_location> <service_account_key>
![image](https://github.com/anuraghambir/Map-Reduce-GCP/assets/66880947/36dc29da-173c-4be2-a084-e43b36ece8ca)
