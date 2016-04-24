#!/bin/bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"

# Local paths must be located under /vagrant, otherwise the docker image can't access them.
# Remote paths should use this syntax: hdfs://hadoop:9000/<path>
docker run -it \
    -v $ROOT:/root \
    -v /tmp:/tmp \
    --link hadoop:hadoop \
    sequenceiq/hadoop-docker:2.7.0 \
    /usr/local/hadoop-2.7.0/bin/hdfs $@
