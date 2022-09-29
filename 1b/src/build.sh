#! /bin/bash

$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main OneB.java
jar cf OneB.jar OneB*.class