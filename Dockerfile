FROM debian:9

RUN \
  apt-get update \

  && apt-get install --no-install-recommends --no-install-suggests -y \
    build-essential \
    ca-certificates \
    cpanminus \
    curl \
    git \
    libclone-perl \
    libcpanel-json-xs-perl \
    libdatetime-perl \
    libev-perl \
    libexpat1 \
    liblog-fast-perl \
    libmodule-install-perl \
    libperl-dev \
    libplack-perl \
    procps \
    python \
    python-dev \
    unzip \

  && curl -o uwsgi_latest_from_installer.tar.gz https://projects.unbit.it/downloads/uwsgi-latest.tar.gz \
  && mkdir uwsgi_latest_from_installer \
  && tar zvxC uwsgi_latest_from_installer --strip-components=1 -f uwsgi_latest_from_installer.tar.gz \
  && cd uwsgi_latest_from_installer \
  && UWSGI_PROFILE=psgi make \
  && cp ./uwsgi /usr/local/bin/uwsgi \
  && cd / \
  && rm -rf /uwsgi_latest_from_installer \

  && cpanm -n \
    https://github.com/Mons/AnyEvent-HTTP-Server-II.git \
    List::MoreUtils \
    List::MoreUtils::XS \
    Text::QueryString \
    Tie::Array::PackedC \

  && apt-get purge -y --auto-remove \
    build-essential \
    ca-certificates \
    cpanminus \
    curl \
    git \
    libperl-dev \
    python \
    python-dev \

  && rm -rf /root/.cpanm \

  && rm -rf /var/lib/apt/lists/*

EXPOSE 80

COPY start.sh /usr/local/bin/start.sh
COPY bin /usr/local/bin
COPY lib /usr/share/perl5

CMD ["/usr/local/bin/start.sh"]
