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
    liblog-fast-perl \
    libmodule-install-perl \
    libplack-perl \
    nginx \
    procps \
    unzip \
    uwsgi \
    uwsgi-plugin-psgi \

  && cpanm -n Text::QueryString \

  && apt-get purge -y --auto-remove \
    build-essential \
    ca-certificates \
    cpanminus \

  && rm -rf /var/lib/apt/lists/*

EXPOSE 80

COPY start.sh /usr/local/bin/start.sh
COPY bin /usr/local/bin
COPY lib /usr/share/perl5
COPY nginx.conf /etc/nginx/nginx.conf

CMD ["/usr/local/bin/start.sh"]
