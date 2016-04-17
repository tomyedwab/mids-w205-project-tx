#!/bin/bash

docker build -t mids-w205/spark /vagrant/docker/spark
docker run -d --name spark mids-w205/spark
