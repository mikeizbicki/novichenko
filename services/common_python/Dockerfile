FROM python:3.8.1-slim-buster

################################################################################
# build the chajda dependency
################################################################################

# get a sane build environment
# these only occupy a few MB, and chajda itself occupies ~1GB,
# so it's not worth the effort of a multistage build to keep these off the prod image
RUN apt-get update && apt-get install -y \
    autoconf \
    gcc \
    git \
    make \
    python3 \
    python3-pip \
    sudo \
    wget

# install a newer version of git
RUN apt-get update && apt-get install -y \
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

# build chajda's other dependencies;
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

# install metahtml's dependencies
ADD https://raw.githubusercontent.com/mikeizbicki/metahtml/master/requirements.txt /tmp/deps/metahtml/requirements.txt
RUN cd /tmp \
 && git clone --filter=blob:none --no-checkout https://github.com/mikeizbicki/metahtml \
 && cd metahtml \
 && git sparse-checkout init \
 && git sparse-checkout set '!/tests/.cache' '/*' \
 && git checkout master \
 && pip3 install --upgrade pip \
 && pip3 install -r requirements.txt \
 && rm -rf /tmp/metahtml

# install chajda
ADD https://api.github.com/repos/mikeizbicki/chajda/git/refs/heads/notes /tmp/chajda.version
RUN cd /tmp \
 && git clone https://github.com/mikeizbicki/chajda \
 && cd chajda \
 && git checkout notes \
 && pip3 install .

# install metahtml; this requires the new version of git
ADD https://api.github.com/repos/mikeizbicki/metahtml/git/refs/heads/master /tmp/metahtml.version
RUN cd /tmp \
 && git clone --filter=blob:none --no-checkout https://github.com/mikeizbicki/metahtml \
 && cd metahtml \
 && git sparse-checkout init \
 && git sparse-checkout set '!/tests/.cache' '/*' \
 && git checkout master \
 && pip3 install --upgrade pip \
 && rm -r tests \
 && pip3 install . \
 && rm -rf /tmp/metahtml
