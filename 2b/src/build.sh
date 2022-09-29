#! /bin/bash

$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main TwoB.java
jar cf TwoB.jar TwoB*.class