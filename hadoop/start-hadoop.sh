#!/bin/bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Uses hadoop docker image from https://hub.docker.com/r/sequenceiq/hadoop-docker/
docker run -d -v $ROOT/core-site.xml:/usr/local/hadoop-2.7.0/etc/hadoop/core-site.xml --name hadoop sequenceiq/hadoop-docker:2.7.0 /etc/bootstrap.sh -d
