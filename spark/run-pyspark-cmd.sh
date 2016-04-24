#!/bin/bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"

# Parameter must be a .py file under root directory
docker run -it --rm \
  --link hadoop:hadoop \
  -v $ROOT:/root \
  -v $ROOT/spark/log4j.properties:/etc/spark/conf/log4j.properties \
  -v /tmp:/tmp \
  --name spark-cmd \
  mids-w205/spark \
  $1
