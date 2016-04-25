#!/bin/bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker run -d \
    -p 80:80 \
    -v $ROOT/server:/usr/share/nginx/html:ro \
    --name nginx \
    nginx:latest
