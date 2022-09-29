#! /bin/bash

$HADOOP_HOME/bin/hadoop fs -rm -r -f /PA1/out_2a_sample
$HADOOP_HOME/bin/hadoop jar TwoA.jar TwoA -D mapreduce.framework.name=yarn /PA1/in/PA1_gutenberg_sample /PA1/out_2a_sample
$HADOOP_HOME/bin/hadoop fs -cat /PA1/out_2a_sample/* >> ../out_sample.txt