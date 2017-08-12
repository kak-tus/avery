FROM debian:9

RUN \
  apt-get update \

  && apt-get install --no-install-recommends --no-install-suggests -y \
    build-essential \
    ca-certificates \
    cpanminus \
    libclone-perl \
    libcpanel-json-xs-perl \
    libdatetime-perl \
    libmodule-install-perl \
    redis-server \
    unzip \

  && cpanm \
    Mojolicious \
    RedisDB \

  && mkdir -p /var/run/redis

EXPOSE 80

COPY redis.conf /etc/redis/redis.conf
COPY start.sh /usr/local/bin/start.sh
COPY bin/avery.pl /usr/local/bin/avery.pl
COPY lib /usr/share/perl5

CMD ["/usr/local/bin/start.sh"]
