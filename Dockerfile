FROM debian:9

RUN \
  apt-get update \

  && apt-get install --no-install-recommends --no-install-suggests -y \
    build-essential \
    ca-certificates \
    cpanminus \
    libanyevent-perl \
    libclone-perl \
    libcpanel-json-xs-perl \
    libdatetime-perl \
    libev-perl \
    libipc-sharelite-perl \
    libsereal-perl \
    unzip \

  && cpanm \
    Mojolicious

EXPOSE 80

COPY start.sh /usr/local/bin/start.sh
COPY bin/avery.pl /usr/local/bin/avery.pl
COPY lib /usr/share/perl5

CMD ["/usr/local/bin/start.sh"]
