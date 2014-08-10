This is a sample of running this program

There are two scripts included in this project.

  - ingest.sh: Uses a rest service to retrieve text files and stores them at the location provided.
      o jt.output.path: use the prefix hdfs:/// if you want the files stored in Hadoop or blank if locally
      o jt.input.uri: use the provided URL to retrieve the data
      o jt.preserve.filepath: boolean true/false if you want to use the URL path as part of the file destination path.
      
  - index.sh:  Only uses one parameter.  This is the directory path to recursively iterate through to include in the
               ElasticSearch index.
             