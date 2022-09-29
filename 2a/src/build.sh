#! /bin/bash

$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main TwoA.java
jar cf TwoA.jar TwoA*.class