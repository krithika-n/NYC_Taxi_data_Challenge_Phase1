#!/bin/bash

cd ~/hadoop-2.7.7/bin
./hadoop fs -mkdir /hdfs
./hadoop fs -mkdir /hdfs/Proj
./hadoop fs -mkdir /hdfs/Proj/Input
./hadoop fs -mkdir /hdfs/Proj/Output
./hadoop fs -copyFromLocal ~/DDS-phase1/src/resources/*csv /hdfs/Proj/Input