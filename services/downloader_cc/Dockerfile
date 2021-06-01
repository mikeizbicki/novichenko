FROM common_python

# set general environment
RUN adduser app
ENV HOME=/home/app
WORKDIR $HOME
ENTRYPOINT ["bash"]

ENV DATADIR=/data/common-crawl
RUN mkdir -p /data/common-crawl \
 && chown app /data/common-crawl \
 && chgrp app /data/common-crawl

# install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    jq \
    parallel \
    procps \
    time
COPY ./requirements.txt $APP_HOME/requirements.txt
RUN pip install --upgrade pip \
 && pip install -r $APP_HOME/requirements.txt

# copy files to container
COPY . $HOME
RUN chown -R app $HOME \
 && chgrp -R app $HOME

# do not change to the app user;
# need root permission to write to mapped volumes
# USER app
