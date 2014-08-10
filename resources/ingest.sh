#!/bin/sh

java -Djt.output.path=hdfs:///in/test -Djt.input.uri="http://www.gutenberg.org/robot/harvest?filetypes[]=txt" -cp ElasticSearch-0.0.1-SNAPSHOT.jar com.jt.GeneralIngest
