#!/usr/bin/env sh

mkdir /tmp/unzip
cp /tmp/data/data.zip /tmp/unzip
cd /tmp/unzip
unzip data.zip > /dev/null
cd /

redis-server /etc/redis/redis.conf

hypnotoad -f /usr/local/bin/avery.pl
