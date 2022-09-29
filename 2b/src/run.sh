#! /bin/bash

$HADOOP_HOME/bin/hadoop fs -rm -r -f /PA1/out_2b
$HADOOP_HOME/bin/hadoop jar TwoB.jar TwoB -D mapreduce.framework.name=yarn /PA1/in/PA1_gutenberg /PA1/out_2b
$HADOOP_HOME/bin/hadoop fs -cat /PA1/out_2b/* >> ../out.txt