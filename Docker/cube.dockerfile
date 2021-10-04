FROM sachinbjohn/ssh-base
RUN apk add --no-cache \
        openjdk8  \
        make \
        g++

RUN wget https://github.com/sbt/sbt/releases/download/v1.2.7/sbt-1.2.7.zip
RUN unzip sbt-1.2.7.zip && mv sbt /opt/sbt && rm sbt-1.2.7.zip
RUN echo "export PATH=/opt/sbt/bin:$PATH" >> /root/.profile
