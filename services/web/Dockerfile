FROM common_python

# set general environment
RUN adduser app
ENV HOME=/home/app
ENV APP_HOME=/home/app/web
RUN mkdir $APP_HOME
WORKDIR $APP_HOME
EXPOSE 5000

# install dependencies
COPY ./requirements.txt $APP_HOME/requirements.txt
RUN pip install --upgrade pip \
 && pip install -r $APP_HOME/requirements.txt

# copy project
COPY . $APP_HOME
RUN chown -R app $APP_HOME \
 && chgrp -R app $APP_HOME

# change to the app user
#USER app
