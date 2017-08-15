FROM debian:9

RUN \
  apt-get update \

  && apt-get install --no-install-recommends --no-install-suggests -y \
    build-essential \
    ca-certificates \
    cpanminus \
    git \
    libanyevent-httpd-perl \
    libcanary-stability-perl \
    libclone-perl \
    libcpanel-json-xs-perl \
    libdatetime-perl \
    libev-perl \
    libhttp-server-simple-perl \
    libipc-sharelite-perl \
    libjson-xs-perl \
    libmemory-usage-perl \
    libsereal-perl \
    unzip \

  && cpanm \
    https://github.com/Mons/AnyEvent-HTTP-Server-II.git \
    Mojolicious \

  && apt-get purge -y --auto-remove \
    build-essential \
    ca-certificates \
    cpanminus \
    git \

  && rm -rf /var/lib/apt/lists/*

EXPOSE 80

COPY start.sh /usr/local/bin/start.sh
COPY bin /usr/local/bin
COPY lib /usr/share/perl5

CMD ["/usr/local/bin/start.sh"]
