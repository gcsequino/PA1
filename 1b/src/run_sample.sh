#! /bin/bash

$HADOOP_HOME/bin/hadoop fs -rm -r -f /PA1/out_1b_sample
$HADOOP_HOME/bin/hadoop jar OneB.jar OneB -D mapreduce.framework.name=yarn /PA1/in/PA1_gutenberg_sample /PA1/out_1b_sample
$HADOOP_HOME/bin/hadoop fs -cat /PA1/out_1b_sample/* >> ../out_sample.txt