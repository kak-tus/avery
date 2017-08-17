#!/usr/bin/env sh

mkdir /tmp/unzip
cp /tmp/data/data.zip /tmp/unzip
cd /tmp/unzip
unzip data.zip > /dev/null
cd /

top -b &

uwsgi --http-socket 0.0.0.0:80 \
  -p 1 -L -l 128 \
  --plugins psgi --psgi /usr/local/bin/avery.psgi
