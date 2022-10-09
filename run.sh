#!/bin/bash
export PATH="$PATH:/usr/home/hadoop/bin"

echo "******************************************************"
echo "****         Deleting output directory            ****"
echo "******************************************************"
rm -R ./output

echo "******************************************************"
echo "****           Running Hadoop in local            ****"
echo "******************************************************"
hadoop jar hadoop-practice.jar -conf ./conf/hadoop-local.xml ./input/ ./output

echo "******************************************************"
echo "****      Execucion result of Hadoop in local     ****"
echo "******************************************************"
cat ./output/*



echo "******************************************************"
echo "****            Creating input in HDFS            ****"
echo "******************************************************"
hadoop fs -rm -R /input
hadoop fs -mkdir /input

echo "******************************************************"
echo "****           Copy input data into HDFS          ****"
echo "******************************************************"
hadoop fs -copyFromLocal ./input/* /input

echo "******************************************************"
echo "****        Deleting output HDFS directory        ****"
echo "******************************************************"
hadoop fs -rm -R /output

echo "******************************************************"
echo "****         Running Hadoop in localhost          ****"
echo "******************************************************"
hadoop jar hadoop-practice.jar -conf ./conf/hadoop-localhost.xml /input /output

echo "******************************************************"
echo "****    Execucion result of Hadoop in localhost   ****"
echo "******************************************************"
hadoop fs -cat /output/*

