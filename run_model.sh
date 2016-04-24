#!/bin/bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

$ROOT/hadoop/hadoop-hdfs.sh dfs -rm -r hdfs://hadoop:9000/models

$ROOT/spark/run-pyspark-cmd.sh /root/model/spark-train-model.py
