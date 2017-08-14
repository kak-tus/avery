FROM debian:9

RUN \
  apt-get update \

  && apt-get install --no-install-recommends --no-install-suggests -y \
    build-essential \
    ca-certificates \
    cpanminus \
    libanyevent-httpd-perl \
    libclone-perl \
    libcpanel-json-xs-perl \
    libdatetime-perl \
    libhttp-server-simple-perl \
    libipc-sharelite-perl \
    libmemory-usage-perl \
    libsereal-perl \
    unzip \

  && cpanm \
    Mojolicious \

  && apt-get purge -y --auto-remove \
    build-essential \
    ca-certificates \
    cpanminus \

  && rm -rf /var/lib/apt/lists/*

EXPOSE 80

COPY start.sh /usr/local/bin/start.sh
COPY bin /usr/local/bin
COPY lib /usr/share/perl5

CMD ["/usr/local/bin/start.sh"]
