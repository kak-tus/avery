FROM debian:9

RUN \
  apt-get update \

  && apt-get install --no-install-recommends --no-install-suggests -y \
    build-essential \
    ca-certificates \
    cpanminus \
    curl \
    git \
    libcpanel-json-xs-perl \
    libdatetime-perl \
    libev-perl \
    liblog-fast-perl \
    procps \
    unzip \

  && cpanm -n \
    https://github.com/Mons/AnyEvent-HTTP-Server-II.git \
    List::MoreUtils \
    List::MoreUtils::XS \
    Tie::Array::PackedC \

  && apt-get purge -y --auto-remove \
    build-essential \
    ca-certificates \
    cpanminus \
    curl \
    git \

  && rm -rf /root/.cpanm \

  && rm -rf /var/lib/apt/lists/*

EXPOSE 80

COPY start.sh /usr/local/bin/start.sh
COPY bin /usr/local/bin
COPY lib /usr/share/perl5

CMD ["/usr/local/bin/start.sh"]
