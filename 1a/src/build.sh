#! /bin/bash

$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main OneA.java
jar cf OneA.jar OneA*.class