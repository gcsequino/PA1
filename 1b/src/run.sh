#! /bin/bash

$HADOOP_HOME/bin/hadoop fs -rm -r -f /PA1/out_1b
$HADOOP_HOME/bin/hadoop jar OneB.jar OneB -D mapreduce.framework.name=yarn /PA1/in/PA1_gutenberg /PA1/out_1b
$HADOOP_HOME/bin/hadoop fs -cat /PA1/out_1b/* >> ../out.txt
