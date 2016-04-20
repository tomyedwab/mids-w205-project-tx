#!/bin/bash

# Parameter must be a .py file under directory /vagrant
docker run -it --rm \
  --link hadoop:hadoop \
  -v /vagrant:/vagrant \
  -v /vagrant/spark/log4j.properties:/etc/spark/conf/log4j.properties \
  --name spark-cmd \
  mids-w205/spark \
  $1
