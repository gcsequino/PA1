#! /bin/bash

$HADOOP_HOME/bin/hadoop fs -rm -r -f /PA1/out_1a
$HADOOP_HOME/bin/hadoop jar OneA.jar OneA -D mapreduce.framework.name=yarn /PA1/in/PA1_gutenberg /PA1/out_1a
$HADOOP_HOME/bin/hadoop fs -cat /PA1/out_1a/* >> ../out.txt
