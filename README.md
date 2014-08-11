ElasticSearchExample
====================

This is a repo of me playing around with pulling files from the Gutenberg doc repository and then creating an ElasticSearch index on them for searches

Before running any of these classes, it is required that you setup the config.properties files or passing in the properties through the Java system properties.

- ingest.sh: Uses a rest service to retrieve text files and stores them at the location provided.
    o jt.output.path: use the prefix hdfs:/// if you want the files stored in Hadoop or blank if locally
    o jt.input.uri: use the provided URL to retrieve the data
    o jt.preserve.filepath: boolean true/false if you want to use the URL path as part of the file destination path.
- index.sh: Only uses one parameter. This is the directory path to recursively iterate through to include in the
ElasticSearch index.
