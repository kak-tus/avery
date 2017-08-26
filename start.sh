#!/usr/bin/env sh

mkdir /tmp/unzip
cp /tmp/data/data.zip /tmp/unzip
cd /tmp/unzip
unzip data.zip > /dev/null
cd /

top -b &

/usr/local/bin/uwsgi --http 0.0.0.0:80 --http-keepalive -p 1 -L -l 128 --psgi /usr/local/bin/avery.psgi
