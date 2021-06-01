ARG BASE_IMAGE_VERSION=13
FROM postgres:$BASE_IMAGE_VERSION AS base

################################################################################
# install generic dependencies for a sane build environment
################################################################################

# install system packages for building postgres extensions
RUN apt-get update && apt-get install -y \
    autoconf \
    gcc \
    less \
    make \
    postgresql-server-dev-13 \
    postgresql-plpython3-13 \
    python3 \
    python3-pip \
    sudo \
    wget

# install a newer version of git
# these libraries are required just for installing a new version of git
RUN apt-get install -y \
    libcurl4-gnutls-dev \
    libexpat1-dev gettext \
    libz-dev \
    libssl-dev \
    asciidoc \
    xmlto \
    docbook2x
RUN cd /tmp \
 && wget https://www.kernel.org/pub/software/scm/git/git-2.30.1.tar.gz \
 && tar -xzf git-2.30.1.tar.gz \
 && cd git-2.30.1 \
 && ./configure \
 && make \
 && make install \
 && rm -rf /tmp/git-2.30.1

################################################################################
# install postgres extensions
################################################################################

# install rum index
RUN cd /tmp \
 && git clone https://github.com/postgrespro/rum \
 && cd rum \
 && git checkout 1.3.7 \
 && make USE_PGXS=1 \
 && make USE_PGXS=1 install \
 && rm -rf /tmp/rum

# build chajda's dependencies;
# these rarely change, so we want to ensure this build step gets cached;
# the ADD commands ensure that the dependencies will get rebuilt whenever they actually change
ADD https://raw.githubusercontent.com/mikeizbicki/chajda/notes/install_dependencies.sh /tmp/deps/chajda/install_dependencies.sh
ADD https://raw.githubusercontent.com/mikeizbicki/chajda/notes/requirements.txt /tmp/deps/chajda/requirements.txt
RUN cd /tmp \
 && git clone https://github.com/mikeizbicki/chajda \
 && cd chajda \
 && git checkout notes \
 && sh install_dependencies.sh \
 && rm -rf /tmp/chajda

# install pgrollup's dependencies
ADD https://raw.githubusercontent.com/mikeizbicki/pgrollup/master/install_dependencies.sh /tmp/deps/pgrollup/install_dependencies.sh
ADD https://raw.githubusercontent.com/mikeizbicki/pgrollup/master/requirements.txt /tmp/deps/pgrollup/requirements.txt
RUN cd /tmp \
 && git clone https://github.com/mikeizbicki/pgrollup \
 && cd pgrollup \
 && sh install_dependencies.sh \
 && rm -rf /tmp/pgrollup

# install chajda from the notes branch
# FIXME:
# should we add a git checkout version?
ADD https://api.github.com/repos/mikeizbicki/chajda/git/refs/heads/notes /tmp/chajda.version
RUN cd /tmp \
 && git clone https://github.com/mikeizbicki/chajda \
 && cd chajda \
 && git checkout notes \
 && pip3 install . \
 && make USE_PGXS=1 \
 && make USE_PGXS=1 install

# install pgrollup
# FIXME:
# should we add a git checkout version?
ADD https://api.github.com/repos/mikeizbicki/pgrollup/git/refs/heads/master /tmp/pg_rollup.version
RUN cd /tmp \
 && git clone https://github.com/mikeizbicki/pgrollup \
 && cd pgrollup \
 && pip3 install . \
 && make USE_PGXS=1 \
 && make USE_PGXS=1 install \
 && rm -rf /tmp/pgrollup

# install metahtml; this requires the new version of git
ADD https://api.github.com/repos/mikeizbicki/metahtml/git/refs/heads/master /tmp/metahtml.version
RUN cd /tmp \
 && git clone --filter=blob:none --no-checkout https://github.com/mikeizbicki/metahtml \
 && cd metahtml \
 && git sparse-checkout init \
 && git sparse-checkout set '!/tests/.cache' '/*' \
 && git checkout master \
 && pip3 install -r requirements.txt \
 && python3 scripts/generate_tests_json.py > /tmp/metahtml/all_tests.json

################################################################################
# setup the postgres server
################################################################################

# create a fastdata directory for a faster tablespace
RUN mkdir /fastdata \
 && chown postgres /fastdata

# copy over the database initial data
COPY ./data /tmp/data
RUN chown postgres -R /tmp/data

# copy over the database creation scripts
COPY ./postgresql.conf /etc/postgresql.conf
COPY ./sql/*.sql /docker-entrypoint-initdb.d/
