#!/bin/bash

# Parameter must be a .py file under directory /vagrant
docker run -it --rm --link hadoop:hadoop -v /vagrant:/vagrant --name spark-cmd mids-w205/spark $1
