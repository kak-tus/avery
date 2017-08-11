FROM debian:9

RUN \
  apt-get update \

  && apt-get install --no-install-recommends --no-install-suggests -y \
    build-essential \
    ca-certificates \
    cpanminus \
    libmodule-install-perl \
    redis-server \
    unzip \

  && cpanm \
    Mojolicious \
    RedisDB \
    Cpanel::JSON::XS \

  && mkdir -p /var/run/redis

EXPOSE 80

COPY redis.conf /etc/redis/redis.conf

CMD ["/usr/local/bin/start.sh"]
