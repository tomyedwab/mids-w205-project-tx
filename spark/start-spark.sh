#!/bin/bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker build -t mids-w205/spark $ROOT/../docker/spark
docker run -d --name spark mids-w205/spark
