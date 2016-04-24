#!/usr/bin/python

import json
import os
import subprocess

class Row(object):
    def __init__(self, **kwargs):
        self.values = kwargs

class ForecastModel(object):
    def __init__(self, root_dir, data_dir):
        self.root_dir = root_dir
        self.data_dir = data_dir

    def generate_predictions(self, dataset_name):
        # Run transform in Spark
        print "Running Spark prediction model..."

        subprocess.call(
            "%s/hadoop/hadoop-hdfs.sh "
            "dfs -mkdir hdfs://hadoop:9000/predictions" % (
                self.root_dir),
            shell=True
        )

        subprocess.call(
            '%s/spark/run-pyspark-cmd.sh '
            '/root/model/spark-predict.py %s' % (
                self.root_dir, dataset_name),
            shell=True
        )

        # Fetch prediction data
        print "Fetching predicted data..."

        subprocess.call(
            "%s/hadoop/hadoop-hdfs.sh "
            "dfs -cat hdfs://hadoop:9000/predictions/%s/part-* "
            " > /tmp/predictions.txt" % (
                self.root_dir, dataset_name),
            shell=True
        )

        rows = []
        with open("/tmp/predictions.txt", "r") as f:
            for line in f:
                rows.append(eval(line))

        # Save to a big JSON blob
        try:
            os.mkdir("%s/server/data" % self.root_dir)
        except OSError:
            pass

        json_blob = json.dumps([row.values for row in rows])
        with open("%s/server/data/latest_predictions.json" % self.root_dir, "w") as f:
            f.write(json_blob)
