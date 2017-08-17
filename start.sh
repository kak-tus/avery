#!/usr/bin/env sh

mkdir /tmp/unzip
cp /tmp/data/data.zip /tmp/unzip
cd /tmp/unzip
unzip data.zip > /dev/null
cd /

top -b &

/usr/local/bin/avery_http_server2.pl
